import os
import re
import csv
import json
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi

# YouTube API setup
with open('shh.txt', 'r') as file:
    API_KEY = file.read().strip()
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Load known channel URLs and IDs from a JSON file
def load_channel_data():
    if os.path.exists('channels_data.json'):
        with open('channels_data.json', 'r', encoding='utf-8') as file:
            return json.load(file)
    return {}

# Save the updated channel URL-ID mappings to a JSON file
def save_channel_data(channel_data):
    with open('channels_data.json', 'w', encoding='utf-8') as file:
        json.dump(channel_data, file, indent=4, ensure_ascii=False)

# Step 1: Extract channel ID from YouTube channel URL
def extract_channel_id(channel_url, known_channels):
    # Check if the channel URL is already known
    if channel_url in known_channels:
        print(f"Found known channel ID for {channel_url}, as it was saved previously.")
        return known_channels[channel_url]

    try:
        # Try extracting from URL directly
        match = re.search(r"youtube\.com\/channel\/([\w-]+)", channel_url)
        if match:
            channel_id = match.group(1)
            known_channels[channel_url] = channel_id  # Save the known mapping
            return channel_id

        # Handle other formats (e.g., user, custom URL)
        match = re.search(r"youtube\.com\/user\/([\w-]+)", channel_url)
        if match:
            username = match.group(1)
            request = youtube.channels().list(part="id", forUsername=username)
            response = request.execute()
            if response['items']:
                channel_id = response['items'][0]['id']
                known_channels[channel_url] = channel_id  # Save the known mapping
                return channel_id

        match = re.search(r"youtube\.com\/(?:c|@)\/([\w-]+)", channel_url)
        if match:
            custom_url = match.group(1)
            request = youtube.search().list(
                part="snippet",
                q=custom_url,
                type="channel",
                maxResults=1
            )
            response = request.execute()
            if response['items']:
                channel_id = response['items'][0]['snippet']['channelId']
                known_channels[channel_url] = channel_id  # Save the known mapping
                return channel_id

        if "youtube.com/@" in channel_url:
            handle = channel_url.split("@")[1]
            request = youtube.search().list(
                part="snippet",
                q=handle,
                type="channel",
                maxResults=1
            )
            response = request.execute()
            if response['items']:
                channel_id = response['items'][0]['snippet']['channelId']
                known_channels[channel_url] = channel_id  # Save the known mapping
                return channel_id

        raise ValueError("Invalid URL format")
    except Exception as e:
        print(f"Error resolving channel ID for {channel_url}: {e}, by requesting from the API.")
        return None

# Step 2: Get video URLs, titles, uploader info, and date for the N most recent videos
def get_recent_videos(channel_id, n):
    videos = []
    print(f"Fetching {n} most recent videos for channel {channel_id}...")

    page_token = None
    while len(videos) < n:
        try:
            request = youtube.search().list(
                part="id,snippet",
                channelId=channel_id,
                maxResults=50,
                order="date",
                type="video",
                pageToken=page_token
            )
            response = request.execute()

            for item in response['items']:
                if len(videos) < n:
                    video_url = f"https://www.youtube.com/watch?v={item['id']['videoId']}"
                    videos.append({
                        "id": item['id']['videoId'],
                        "title": item['snippet']['title'],
                        "video_url": video_url,
                        "uploader_name": item['snippet']['channelTitle'],
                        "date_uploaded": item['snippet']['publishedAt']
                    })
                else:
                    break

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        except Exception as e:
            print(f"Could not fetch videos for channel {channel_id}: {e}")
            break

    print(f"Found {len(videos)} videos for channel {channel_id}.")
    return videos

# Step 3: Fetch transcripts for the given videos
def fetch_transcripts(videos, existing_transcripts):
    """
    Fetch transcripts for a list of videos, including titles, uploader info, and timecodes.
    Only fetch the transcript if it has not been already fetched.
    """
    transcripts = {}
    errors = {}
    for video in videos:
        video_id = video['id']
        video_title = video['title']
        uploader_name = video['uploader_name']
        video_url = video['video_url']
        date_uploaded = video['date_uploaded']
        combined_key = f"{video_title} ({video_url}) [{date_uploaded}]"

        # Check if the transcript already exists
        if combined_key in existing_transcripts:
            print(f"Skipping {video_title} ({video_id}) as it's already fetched.")
            continue  # Skip this video since it's already fetched

        # If transcript is not found, fetch it
        try:
            print(f"Fetching transcript for {video_title} ({video_id})...")
            transcript = YouTubeTranscriptApi.get_transcript(video_id)

            # Format the transcript with timecodes every minute
            formatted_transcript = []
            current_minute = -1  # Track the last minute added

            for item in transcript:
                start_time = int(item['start'])  # Get start time in seconds
                minute = start_time // 60  # Calculate minute
                hour = start_time // 3600  # Calculate hour

                # Add timecode if the minute has advanced
                if minute != current_minute:
                    if hour > 0:  # Format timecode with hours if necessary
                        minute = minute % 60  # Get minute within the hour
                        timecode = f"[{hour}:{minute:02d}]"
                    else:
                        timecode = f"[{minute}]"
                    
                    formatted_transcript.append(timecode)  # Add timecode
                    current_minute = minute  # Update current minute

                # Add the transcript text
                formatted_transcript.append(item['text'])

            # Join the transcript with spaces to include timecodes correctly
            final_transcript = " ".join(formatted_transcript)

            # Save the transcript
            transcripts[combined_key] = {
                "video_id": video_id,
                "title": video_title,
                "uploader_name": uploader_name,
                "video_url": video_url,
                "date_uploaded": date_uploaded,
                "transcript": final_transcript  # Save formatted transcript with timecodes
            }

        except Exception as e:
            errors[combined_key] = {
                "error": str(e),
                "video_id": video_id,
                "title": video_title,
                "uploader_name": uploader_name,
                "video_url": video_url,
                "date_uploaded": date_uploaded
            }

    return transcripts, errors


# Step 4: Save transcripts and errors to separate JSON files
def save_to_json(transcripts, errors, output_file, error_file):
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(transcripts, file, indent=4, ensure_ascii=False)
    
    if errors:
        with open(error_file, 'w', encoding='utf-8') as file:
            json.dump(errors, file, indent=4, ensure_ascii=False)

# Check if the transcripts.json file exists
def check_if_file_exists(output_file):
    return os.path.exists(output_file)

# Main script
if __name__ == "__main__":
    csv_file = "channels.csv"
    output_file = "transcripts.json"
    error_file = "errors.json"

    # Load the known channel data from the JSON file
    known_channels = load_channel_data()

    if os.path.exists(output_file):
        print(f"{output_file} exists. Checking for already fetched videos...")
        with open(output_file, 'r', encoding='utf-8') as file:
            existing_transcripts = json.load(file)
    else:
        proceed = input(f"{output_file} does not exist. Do you want to proceed and create it? (y/n): ").strip().lower()
        if proceed != 'y':
            print("Operation canceled.")
            exit()
        existing_transcripts = {}

    channel_urls = []
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            channel_urls.append(row['channel_url'])

    print(f"Found {len(channel_urls)} channel URLs in the CSV.")

    all_transcripts = existing_transcripts.copy()
    all_errors = {}

    for channel_url in channel_urls:
        print(f"Processing channel: {channel_url}")

        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row['channel_url'] == channel_url:
                    vids_fetched = int(row['vids_fetched'])
                    break

        print(f"Fetching {vids_fetched} most recent videos for channel {channel_url}...")
        channel_id = extract_channel_id(channel_url, known_channels)
        if not channel_id:
            continue

        videos = get_recent_videos(channel_id, vids_fetched)
        if not videos:
            continue

        transcripts, errors = fetch_transcripts(videos, existing_transcripts)

        for combined_key, data in transcripts.items():
            all_transcripts[combined_key] = data

        all_errors.update(errors)

    # Save the updated channel mappings and transcripts
    save_channel_data(known_channels)
    save_to_json(all_transcripts, all_errors, output_file, error_file)
    print(f"Transcripts saved to {output_file}.")
    if all_errors:
        print(f"Errors saved to {error_file}.")
