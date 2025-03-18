import re
from flask import Flask, render_template, request
import psycopg2

from getData import get_db_connection

app = Flask(__name__)


# Function to clean transcripts for search comparison (ignoring timecodes)
def clean_transcript_for_search(text):
    # Remove any timecode format like [00:01:23] or [0:12] for search purposes
    return re.sub(r'\[.*?\]', '', text)

# Function to bold search query in the transcript
def bold_query_in_transcript(text, query):
    # Escape query to handle special regex characters
    escaped_query = re.escape(query)
    # Bold the search query
    return re.sub(rf'({escaped_query})', r'<b>\1</b>', text, flags=re.IGNORECASE)

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
            SELECT v.video_id, v.title, t.start_time, t.transcript
            FROM videos v
            JOIN transcripts t ON v.video_id = t.video_id;
        ''')
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Loop through the transcripts and search for the query
        for row in rows:
            video_id, title, start_time, transcript = row
            cleaned_transcript = clean_transcript_for_search(transcript)  # Clean for search comparison
            if re.search(query, cleaned_transcript, re.IGNORECASE):  # Case-insensitive search
                # Bold the query in the original transcript for results
                bolded_transcript = bold_query_in_transcript(transcript, query)
                results.append({
                    'video_id': video_id,
                    'title': title,
                    'start_time': start_time,
                    'transcript': bolded_transcript  # Store the bolded transcript
                })

    return render_template('search.html', results=results)

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

# Show transcript for a video
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

