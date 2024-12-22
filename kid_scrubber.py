import json
from datetime import datetime

# Keywords to keep the transcript
KEYWORDS = ["boogie", "boogie2988", "dezi"]

# Load transcripts from the input file
def load_transcripts(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

# Save the filtered transcripts to the output file
def save_transcripts(transcripts, file_path):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(transcripts, file, ensure_ascii=False, indent=4)

# Check if any keyword exists in the given text
def contains_keywords(text, keywords):
    text = text.lower()
    return any(keyword in text for keyword in keywords)

# Filter transcripts based on the criteria
def filter_transcripts(input_file, output_file):
    transcripts = load_transcripts(input_file)
    filtered_transcripts = {}

    for key, data in transcripts.items():
        uploader = data.get("uploader_name", "").lower()
        title = data.get("title", "")
        transcript = data.get("transcript", "")

        # Filter out KidBehindACamera unless keywords are in the title or transcript
        if uploader == "kidbehindacamera" and not (
            contains_keywords(title, KEYWORDS) or contains_keywords(transcript, KEYWORDS)
        ):
            continue

        filtered_transcripts[key] = data

    save_transcripts(filtered_transcripts, output_file)

if __name__ == "__main__":
    input_file = "transcripts.json"
    date_suffix = datetime.now().strftime("%Y-%m-%d")
    output_file = f"reduced_kid_transcripts_{date_suffix}.json"

    filter_transcripts(input_file, output_file)
    print(f"Filtered transcripts saved to {output_file}")