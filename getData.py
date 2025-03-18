import re
import psycopg2
from psycopg2.extras import DictCursor
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
import csv

# Database connection setup
def get_db_connection():
    return psycopg2.connect(
        dbname="youtube_librarian_data",
        user=dbuser,
        password=dbpass,
        host="127.0.0.1",
        port="5432"
    )

# Initialize YouTube API
def load_secrets():
    with open('shh.txt', 'r') as file:
        api_key = file.readline().strip()
        dbuser = file.readline().strip()
        dbpass = file.readline().strip()
    return api_key, dbuser, dbpass

api_key, dbuser, dbpass = load_secrets()
youtube = build('youtube', 'v3', developerKey=api_key)

# Ensure tables exist
def initialize_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    channel_id TEXT UNIQUE NOT NULL
                );
                CREATE TABLE IF NOT EXISTS videos (
                    video_id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL REFERENCES channels(channel_id),
                    title TEXT NOT NULL,
                    video_url TEXT NOT NULL,
                    uploader_name TEXT NOT NULL,
                    date_uploaded TIMESTAMP NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transcripts (
                    video_id TEXT NOT NULL REFERENCES videos(video_id),
                    start_time INT NOT NULL,
                    transcript TEXT NOT NULL,
                    PRIMARY KEY (video_id, start_time)
                );
            """)
            conn.commit()

# Fetch channel ID from DB or API
def get_channel_id(channel_url):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT channel_id FROM channels WHERE url = %s", (channel_url,))
            row = cur.fetchone()
            if row:
                return row['channel_id']

            channel_id = extract_channel_id_from_api(channel_url)
            if channel_id:
                cur.execute("INSERT INTO channels (url, channel_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (channel_url, channel_id))
                conn.commit()
                return channel_id
    return None

# Extract channel ID using YouTube API
def extract_channel_id_from_api(channel_url):
    try:
        match = re.search(r"youtube\.com\/channel\/([\w-]+)", channel_url)
        if match:
            return match.group(1)

        # Handle other formats (user, custom URL, handle)
        request = youtube.search().list(part="snippet", q=channel_url.split("/")[-1], type="channel", maxResults=1)
        response = request.execute()
        if response['items']:
            return response['items'][0]['snippet']['channelId']
    except Exception as e:
        print(f"Error resolving channel ID: {e}")
    return None

# Fetch videos from API and store them
def get_recent_videos(channel_id, n):
    videos = []
    request = youtube.search().list(
        part="id,snippet",
        channelId=channel_id,
        maxResults=n,
        order="date",
        type="video"
    )
    response = request.execute()
    for item in response['items']:
        videos.append({
            "video_id": item['id']['videoId'],
            "title": item['snippet']['title'],
            "video_url": f"https://www.youtube.com/watch?v={item['id']['videoId']}",
            "uploader_name": item['snippet']['channelTitle'],
            "date_uploaded": item['snippet']['publishedAt']
        })
    store_videos(channel_id, videos)
    return videos

# Store videos in the database
def store_videos(channel_id, videos):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for video in videos:
                cur.execute("""
                    INSERT INTO videos (video_id, channel_id, title, video_url, uploader_name, date_uploaded)
                    VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """, (video['video_id'], channel_id, video['title'], video['video_url'], video['uploader_name'], video['date_uploaded']))
            conn.commit()



# Fetch and store transcripts
def fetch_and_store_transcripts():
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT video_id FROM videos WHERE video_id NOT IN (SELECT DISTINCT video_id FROM transcripts)")
            videos = cur.fetchall()
            for video in videos:
                try:
                    transcript = YouTubeTranscriptApi.get_transcript(video['video_id'])
                    segments = split_transcript_into_segments(transcript, 1800)  # 1800 seconds = 30 minutes
                    for start_time, segment in segments.items():
                        formatted_transcript = " ".join([f"[{int(item['start']) // 60}:{int(item['start']) % 60}] {item['text']}" for item in segment])
                        cur.execute("INSERT INTO transcripts (video_id, start_time, transcript) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (video['video_id'], start_time, formatted_transcript))
                    conn.commit()
                except Exception as e:
                    print(f"Failed to fetch transcript for {video['video_id']}: {e}")

def split_transcript_into_segments(transcript, segment_length):
    segments = {}
    current_segment = []
    current_start_time = 0
    for item in transcript:
        if item['start'] >= current_start_time + segment_length:
            segments[current_start_time] = current_segment
            current_segment = []
            current_start_time += segment_length
        current_segment.append(item)
    if current_segment:
        segments[current_start_time] = current_segment
    return segments

if __name__ == "__main__":
    initialize_db()
    # Read channel URLs from targets.csv
    channel_urls = []
    with open('targets.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            channel_urls.append(row[0])
    for channel_url in channel_urls:
        channel_id = get_channel_id(channel_url)
        if channel_id:
            videos = get_recent_videos(channel_id, 50)
    fetch_and_store_transcripts()
