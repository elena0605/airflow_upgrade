import csv

def read_channel_ids_from_csv(file_path):
    user_channel_mapping = {}
    with open(file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            username = row["username"]
            channel_id = row["channel_id"]
            user_channel_mapping[username] = channel_id
    return user_channel_mapping

def read_usernames_from_csv(file_path):
    """Reads usernames from a local CSV file."""
    usernames = []
    with open(file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            usernames.append(row["username"])
    return usernames   