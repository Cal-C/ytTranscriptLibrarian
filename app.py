import re
from flask import Flask, render_template, request
import psycopg2
from datetime import datetime


from getData import get_db_connection

app = Flask(__name__)

# Function to clean transcripts for search comparison (ignoring timecodes)
def clean_transcript_for_search(text):
    return re.sub(r'\[.*?\]', '', text)  # Remove timecodes like [00:01:23]

# Function to bold search query in the transcript
def bold_query_in_transcript(text, query):
    escaped_query = re.escape(query)  # Escape for regex
    return re.sub(rf'({escaped_query})', r'<b>\1</b>', text, flags=re.IGNORECASE)

# Function to filter transcript snippets around search occurrences
def extract_relevant_snippets(transcript_data, query):
    query_regex = re.compile(re.escape(query), re.IGNORECASE)
    occurrences = {}

    # Find all matches in the transcript
    for video_id, title, uploader_name, date_uploaded, start_time, transcript in transcript_data:
        cleaned_transcript = clean_transcript_for_search(transcript)
        matches = list(query_regex.finditer(cleaned_transcript))
        if matches:
            if video_id not in occurrences:
                # Format date_uploaded to display date and hour in AM/PM notation
                formatted_date_uploaded = date_uploaded.strftime('%Y-%m-%d %I%p')
                occurrences[video_id] = {
                    'title': title,
                    'uploader_name': uploader_name,
                    'date_uploaded': formatted_date_uploaded,
                    'cleaned_transcript': cleaned_transcript,
                    'matches': []
                }
            for match in matches:
                occurrences[video_id]['matches'].append((match.start(), match.end()))

    results = []
    for video_id, data in occurrences.items():
        title = data['title']
        uploader_name = data['uploader_name']
        date_uploaded = data['date_uploaded']
        cleaned_transcript = data['cleaned_transcript']
        matches = data['matches']

        snippets = []
        for match_start, match_end in matches:
            snippet_start = max(0, match_start - 500)  # Approximate 2 min before
            snippet_end = min(len(cleaned_transcript), match_end + 500)  # Approximate 2 min after
            snippet = cleaned_transcript[snippet_start:snippet_end]
            snippet = bold_query_in_transcript(snippet, query)
            snippets.append(snippet)

        results.append({
            'video_id': video_id,
            'title': title,
            'uploader_name': uploader_name,
            'date_uploaded': date_uploaded,
            'snippets': snippets
        })

    return results

# Search function for transcripts
@app.route('/search', methods=['GET', 'POST'])
def search():
    query = request.form.get('query')  # Search query (from form)
    results = []
    if query:
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch all video transcripts
        cur.execute('''
            SELECT v.video_id, v.title, v.uploader_name, v.date_uploaded, t.start_time, t.transcript
            FROM videos v
            JOIN transcripts t ON v.video_id = t.video_id;
        ''')
        rows = cur.fetchall()
        cur.close()
        conn.close()

        results = extract_relevant_snippets(rows, query)

    return render_template('search.html', results=results, query=query)

# Home page to list channels
@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM channels;')
    channels = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('index.html', channels=channels)

# Show videos for a specific channel
@app.route('/channel/<channel_id>')
def channel(channel_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT * FROM videos
        WHERE channel_id = %s;
    ''', (channel_id,))
    videos = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('channel.html', videos=videos)

# Show full transcript for a video
@app.route('/video/<video_id>')
def video(video_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT v.title, v.video_url, t.start_time, t.transcript
        FROM videos v
        JOIN transcripts t ON v.video_id = t.video_id
        WHERE v.video_id = %s
        ORDER BY t.start_time;
    ''', (video_id,))
    video_transcripts = cur.fetchall()
    cur.close()
    conn.close()
    return render_template('video.html', video_transcripts=video_transcripts)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
