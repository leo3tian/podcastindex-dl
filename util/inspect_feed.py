import feedparser
import requests
import sys

# --- Configuration ---
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
REQUEST_TIMEOUT = 15

# --- Content Filtering (Copied from worker.py) ---
# These settings control the heuristic scoring to filter out non-dialogue content.
CATEGORY_SCORES = {
    # Positive signals (dialogue-focused)
    'news': 20, 'commentary': 20, 'politics': 15, 'history': 15,
    'science': 15, 'technology': 15, 'business': 15, 'education': 10,
    'courses': 10, 'sports': 10, 'society & culture': 10, 'comedy': 10,
    'comedy interview': 15, 'crime': 10, 'arts': 5,
    'music commentary': 5, 'music interviews': 10,
    
    # Negative signals (likely non-dialogue)
    'spirituality': -10, 'religion & spirituality': -5,
    'music': -30
}

POSITIVE_KEYWORDS = {
    'interview': 15, 'discussion': 15, 'conversation': 10, 'commentary': 10,
    'panel': 10, 'explains': 10, 'breaks down': 10, 'analyzes': 10,
    'hosted by': 5, 'episode': 2
}

NEGATIVE_KEYWORDS = {
    # Music formats
    'dj set': -25, 'dj mix': -25, 'tracklist': -20, 'remix': -15,
    'live set': -15, 'mixtape': -15,
    # Music genres
    'techno': -20, 'house music': -20, 'trance': -20, 'edm': -20,
    'ambient': -15, 'instrumental': -15,
    # Other non-dialogue
    'soundscape': -30, 'asmr': -25, 'binaural beats': -25,
    'guided meditation': -15, 'healing frequencies': -20
}

# A feed's final score must be zero or higher to be included.
MINIMUM_SCORE_THRESHOLD = 0

def calculate_dialogue_score(feed_info):
    """Calculates a heuristic score to guess if a feed is dialogue-based."""
    # Normalize text fields for case-insensitive matching.
    feed_title = feed_info.get('title', '').lower()
    feed_summary = feed_info.get('summary', '').lower()
    feed_categories = [tag.get('term', '').lower() for tag in feed_info.get('tags', [])]
    
    score = 0
    
    # 1. Score based on categories.
    for cat, cat_score in CATEGORY_SCORES.items():
        if cat in feed_categories:
            score += cat_score
            
    # 2. Score based on positive keywords in summary/title.
    for keyword, keyword_score in POSITIVE_KEYWORDS.items():
        if keyword in feed_summary or keyword in feed_title:
            score += keyword_score
            
    # 3. Score based on negative keywords in summary/title.
    for keyword, keyword_score in NEGATIVE_KEYWORDS.items():
        if keyword in feed_summary or keyword in feed_title:
            score += keyword_score
            
    return score

def print_dict_nicely(d, indent=0):
    """Recursively prints a dictionary with indentation for readability."""
    for key, value in d.items():
        if isinstance(value, dict):
            print('  ' * indent + f"{key}:")
            print_dict_nicely(value, indent + 1)
        elif isinstance(value, list):
            print('  ' * indent + f"{key}:")
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    print('  ' * (indent + 1) + f"[{i}]:")
                    print_dict_nicely(item, indent + 2)
                else:
                    print('  ' * (indent + 1) + f"- {item}")
        else:
            print('  ' * indent + f"{key}: {value}")

def main():
    """
    Main function to fetch, parse, and score an RSS feed's metadata.
    """
    if len(sys.argv) < 2:
        print("Error: Please provide an RSS feed URL as a command-line argument.")
        print("Usage: python inspect_feed.py \"<your_rss_feed_url>\"")
        sys.exit(1)

    rss_url = sys.argv[1]
    print(f"--- Fetching and parsing feed: {rss_url} ---\n")

    try:
        response = requests.get(rss_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        feed_info = feed.get('feed', {})

        if feed.bozo:
            print("--- WARNING: This feed may be malformed. ---\n")
            print(f"Bozo Exception: {feed.bozo_exception}\n")

        # --- Calculate and Display Score ---
        score = calculate_dialogue_score(feed_info)
        
        print("\n" + "="*50)
        print(" Content Filtering Score")
        print("="*50)
        print(f"Final Score: {score}")
        
        if score >= MINIMUM_SCORE_THRESHOLD:
            print(f"Result: INCLUDED (Score is >= {MINIMUM_SCORE_THRESHOLD})")
        else:
            print(f"Result: FILTERED (Score is < {MINIMUM_SCORE_THRESHOLD})")
        print("="*50)

        # --- Print Top-Level Feed Metadata ---
        print("\n" + "="*50)
        print(" Overall Feed Information")
        print("="*50)
        print_dict_nicely(feed_info)
        
        # --- Print Metadata for the First Episode ---
        if feed.entries:
            print("\n" + "="*50)
            print(" First Episode Information")
            print("="*50)
            print_dict_nicely(feed.entries[0])
        else:
            print("\nNo episodes found in this feed.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching the RSS feed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 