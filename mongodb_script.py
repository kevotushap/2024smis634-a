from retry import retry
from pymongo import MongoClient
from time import sleep
from pymongo.errors import AutoReconnect

# Replace '<username>' and '<password>' with your actual MongoDB Atlas username and password
username = 'kevotushap'
password = 'wbrEh2ouzYHWe9bv'
cluster_url = 'mongodb+srv://kevotushap:<password>@2024smis634-a.tyr28wn.mongodb.net/'.format(username, password)

# Connect to MongoDB server
def connect_to_mongodb():
    client = MongoClient('mongodb+srv://kevotushap:wbrEh2ouzYHWe9bv@2024smis634-a.tyr28wn.mongodb.net/')
    return client

# Define retry function
def retry(func, max_retries=5, delay=1):
    retries = 0
    while retries < max_retries:
        try:
            return func()
        except AutoReconnect:
            retries += 1
            if retries == max_retries:
                raise
            sleep(delay)

# Call the function to establish the connection
client = connect_to_mongodb()

# Once connected, access the database and collection
db = client['fb_sample_hwk']
collection = db['hwkCollection']

# Task 1: Count posts from accounts "eBay" and "Disney" with likes > 10
task_1_count = collection.count_documents({
    "$and": [
        {"$or": [{"Page Name": "eBay"}, {"Page Name": "Disney"}]},
        {"Likes": {"$gt": 10}}
    ]
})

# Task 2: Find posts with likes > 3 or shares > 5, display "Page Name", "Likes", and "Shares"
task_2_results = collection.find({
    "$or": [
        {"Likes": {"$gt": 3}},
        {"Shares": {"$gt": 5}}
    ]
}, {"Page Name": 1, "Likes": 1, "Shares": 1})

# Task 3: Find top 3 Facebook accounts with the largest number of likes, display "Page Name" and "Likes"
task_3_results = collection.aggregate([
    {"$sort": {"Likes": -1}},
    {"$limit": 3},
    {"$project": {"Page Name": 1, "Likes": 1, "_id": 0}}
])

# Task 4: Count documents with the field "Image Text"
task_4_count = collection.count_documents({"Image Text": {"$exists": True}})

# Task 5: Find top 3 Facebook accounts starting or ending with "d" with  the largest Interaction Ratios
task_5_results = collection.aggregate([
    {
        "$match": {
            "$or": [
                {"Page Name": {"$regex": "^d", "$options": "i"}},
                {"Page Name": {"$regex": "d$", "$options": "i"}}
            ]
        }
    },
    {
        "$addFields": {
            "InteractionRatio": {
                "$cond": {
                    "if": {"$eq": ["$Total Views", 0]},
                    "then": 0,  # Return 0 if Total Views is zero
                    "else": {"$divide": [{"$add": ["$Likes", "$Shares"]}, "$Total Views"]}
                }
            }
        }
    },
    {
        "$sort": {"InteractionRatio": -1}
    },
    {
        "$limit": 3
    }
])

# Display results
print("Task 1: Number of posts from 'eBay' and 'Disney' with likes > 10:", task_1_count)
print("\nTask 2: Posts with likes > 3 or shares > 5 (Page Name, Likes, Shares):")
for post in task_2_results:
    print(post)
print("\nTask 3: Top 3 Facebook accounts with the largest number of likes (Page Name, Likes):")
for account in task_3_results:
    print(account)
print("\nTask 4: Number of documents with the field 'Image Text':", task_4_count)
print("\nTask 5: Top 3 Facebook accounts starting or ending with 'd' with largest Interaction Ratios:")
for account in task_5_results:
    print(account)

# Close MongoDB connection
client.close()

