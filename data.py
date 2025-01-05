import csv
from astrapy import DataAPIClient
import uuid
import os

client = DataAPIClient(os.environ.get("API_KEY"))
db = client.get_database_by_api_endpoint(os.environ.get("DB_URL"))
collection = db.get_collection("social_media_engagement")
csv_file_path = "engagement.csv"
with open(csv_file_path, mode='r') as file:
    csv_reader = csv.DictReader(file)

    for row in csv_reader:
        post_id = str(uuid.uuid4())
        post_type = row["post_type"]
        likes = int(row["likes"])
        shares = int(row["shares"])
        comments = int(row["comments"])

        collection.insert_one({
            "post_id": post_id,
            "post_type": post_type,
            "likes": likes,
            "shares": shares,
            "comments": comments
        })

print("Data successfully uploaded to AstraDB.")
