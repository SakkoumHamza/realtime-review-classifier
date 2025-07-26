import kagglehub
import os, shutil

RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "../data/")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def download_kindle_reviews(): # kindle book reviews

    path = kagglehub.dataset_download("bharadwaj6/kindle-reviews")
    print("Kindle Reviews downloaded to:", path)

    json_file_path = os.path.join(path, "kindle_reviews.json")
    if os.path.exists(json_file_path):
        shutil.move(json_file_path, os.path.join(RAW_DATA_DIR, "kindle_reviews.json"))
        print("JSON file moved to:", RAW_DATA_DIR)
    else:
        print("JSON file not found in the downloaded dataset.")



if __name__ == "__main__":
    download_kindle_reviews()
