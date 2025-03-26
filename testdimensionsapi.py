import requests
import pandas as pd
import json
import time
import re
from urllib.parse import urlparse

def extract_figshare_id_and_host(url):
    """Extracts the host site (e.g., tandf, plos) and the last numeric article ID from the Source linkout URL."""
    parsed_url = urlparse(url)
    hostname_parts = parsed_url.netloc.split(".")  # Split hostname by dots

    if "figshare" in hostname_parts:
        try:
            host_site = hostname_parts[0]  # Extract host site name (e.g., tandf, plos)
            article_id = re.search(r'(\d+)$', url)  # Extract last numeric sequence
            if article_id:
                return host_site, article_id.group(1)
        except IndexError:
            pass

    return None, None  # If invalid Figshare URL, return None

def get_figshare_metrics(host_site, article_id, max_retries=3):
    """Fetches views, downloads, shares, and citations from the correct Figshare API based on the host site."""
    BASE_URL = f"https://stats.figshare.com/{host_site}/total/article"

    for attempt in range(max_retries):
        try:
            stats_url = f"{BASE_URL}/{article_id}"
            print(f"🔍 Requesting Stats: {stats_url}")

            response = requests.get(stats_url, timeout=15)
            if response.status_code == 200:
                data = json.loads(response.text)
                views = data.get("views", 0)
                downloads = data.get("downloads", 0)
                shares = data.get("shares", 0)
                citations = data.get("cites", 0)

                print(f"✅ Success: Views = {views}, Downloads = {downloads}, Shares = {shares}, Citations = {citations}\n")
                return views, downloads, shares, citations
            
            print(f"⚠️ Attempt {attempt+1}: Error {response.status_code}. Retrying...")

        except requests.exceptions.Timeout:
            print(f"⏳ Attempt {attempt+1}: Request timed out. Retrying in 5 seconds...\n")
            time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"❌ Attempt {attempt+1}: Request failed - {e}\n")
            return None, None, None, None
    
    print(f"❌ Failed to get data for {article_id} at {host_site} after {max_retries} attempts.\n")
    return None, None, None, None

# Load dataset
df = pd.read_csv("Updated_Dimensions_Dataset.csv")

# Extract only rows where 'Source linkout' contains 'figshare'
df_figshare = df[df["Source linkout"].str.contains("figshare", na=False)].copy()

# Add new columns if they don't exist
for col in ["views", "downloads", "shares", "citations"]:
    if col not in df.columns:
        df[col] = None

# Store data in a new CSV file as well
metrics_data = []

for index, row in df_figshare.iterrows():
    host_site, article_id = extract_figshare_id_and_host(row["Source linkout"])
    
    # Skip invalid Figshare links
    if host_site is None or article_id is None:
        print(f"⚠️ Skipping invalid Figshare link: {row['Source linkout']}")
        continue

    views, downloads, shares, citations = get_figshare_metrics(host_site, article_id)

    # Update main dataset
    df.at[index, "views"] = views
    df.at[index, "downloads"] = downloads
    df.at[index, "shares"] = shares
    df.at[index, "citations"] = citations

    # Save for the backup CSV
    metrics_data.append({
        "host_site": host_site,
        "article_id": article_id,
        "source_linkout": row["Source linkout"],
        "views": views,
        "downloads": downloads,
        "shares": shares,
        "citations": citations
    })
    
    time.sleep(2)  # Respect rate limits

# Save updated dataset with metrics
df.to_csv("Updated_Dimensions_Dataset.csv", index=False)
print("✅ Updated 'Updated_Dimensions_Dataset.csv' with Figshare metrics!")

# Save a separate CSV file with just the metrics
metrics_df = pd.DataFrame(metrics_data)
metrics_df.to_csv("Figshare_Metrics.csv", index=False)
print("✅ Created 'Figshare_Metrics.csv' with extracted metrics!")
