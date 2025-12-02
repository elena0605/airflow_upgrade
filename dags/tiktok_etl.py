from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import csv
import json
import logging
import time
from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable    
from bson import ObjectId
from neo4j import GraphDatabase

# Variables will be accessed when needed inside functions
# TIKTOK_CLIENT_KEY = Variable.get("TIKTOK_CLIENT_KEY")
# TIKTOK_CLIENT_SECRET = Variable.get("TIKTOK_CLIENT_SECRET")


# Set up logging - log to airflow logs & console
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.DEBUG)  # Set the log level
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
if not logger.hasHandlers():  # Avoid duplicate handlers
    logger.addHandler(stream_handler)



def generate_date_ranges(start_date, end_date, days_range=30):
    ranges = []
    while start_date < end_date:
        range_end = min(start_date + timedelta(days=days_range - 1), end_date)
        ranges.append((start_date.strftime("%Y%m%d"), range_end.strftime("%Y%m%d")))
        start_date = range_end + timedelta(days=1)
    return ranges

def load_token_from_airflow():
    """    
    Load token information from Airflow variables.
    """
    try:
        token = Variable.get("TIKTOK_TOKEN")
    except:
        token = None
    
    try:
        expires_at = Variable.get("TIKTOK_TOKEN_EXPIRES_AT")
    except:
        expires_at = None

    if expires_at is not None:
        try:
            expires_at = float(expires_at)
        except (ValueError, TypeError):
            logger.warning(f"Invalid TIKTOK_TOKEN_EXPIRES_AT value: {expires_at}. Resetting to None.")
            expires_at = None

    return {
        "token": token,
        "expires_at": expires_at
    }


def save_token_to_airflow(token_info):
    """
    Save token information to Airflow variables.
    """
    Variable.set("TIKTOK_TOKEN", token_info["token"])
    Variable.set("TIKTOK_TOKEN_EXPIRES_AT", str(token_info["expires_at"]))



def get_new_token(client_key, client_secret):
    """
    Fetch a new token using TikTok API.

    """
    logger.info("Fetching new token...")
    url = "https://open.tiktokapis.com/v2/oauth/token/"
    body = {
        "client_key": client_key,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        logger.info(f"Requesting TikTok API with URL: {url}, Headers: {headers}, Body: {body}")
        response = requests.post(url, headers=headers, data=body)
        response.raise_for_status()
        resp = response.json()

        # Calculate the expiration time as a UNIX timestamp
        expires_at = time.time() + resp["expires_in"]

        token_info = {
            "token": "Bearer " + resp["access_token"],
            "expires_at": expires_at
        }

        # Save the token and expiration time to airflow
        save_token_to_airflow(token_info)

        logger.info(f"New token obtained, expires at {datetime.fromtimestamp(expires_at)}")
        return token_info

    except Exception as e:
        logger.error(f"Failed to fetch token: {e}", exc_info=True)
        raise

def is_token_expired(token_info):
    """
    Check if the current token is expired.
    """
    logger.info(f"Checking token expiry: {token_info}")
    if not token_info.get("expires_at"):
        return True
    return time.time() >= token_info["expires_at"]  


def tiktok_get_user_info(username: str, output_dir:str, **context):
    if context is None:
        context = {} 
    # Load the token
    token_info = load_token_from_airflow()

    if is_token_expired(token_info):
         client_key = Variable.get("TIKTOK_CLIENT_KEY")
         client_secret = Variable.get("TIKTOK_CLIENT_SECRET")
         token_info = get_new_token(client_key, client_secret)

    logger.info(f"Now in function tiktok_get_user_info, getting {username}")
    url = 'https://open.tiktokapis.com/v2/research/user/info/'
    params = {"fields": "display_name, bio_description, is_verified, follower_count, following_count, likes_count, video_count, bio_url, avatar_url"}
    body = {"username": username}
    headers = {"Authorization": token_info["token"], "Content-Type" : "application/json"}
    
    try:
        logger.info(f"Requesting TikTok API with URL: {url}, Headers: {headers}, Body: {body}")
        # request
        response = requests.request("POST", url, headers = headers, params = params ,json = body,  timeout=10)
        logger.info("Call is done...")
        # logger.info(f"TikTok API raw response: {response.text}")

        # Parse the response as JSON
        resp_json = response.json()
 
        # Handle specific error case for invalid username
        if response.status_code == 400 and resp_json.get("error", {}).get("code") == "invalid_params":
            logger.info(f"Username {username} cannot be retrieved — probably deleted, private, or invalid.") 
            return None

        # Check for other HTTP errors
        response.raise_for_status()  # This will raise an exception for other 4xx/5xx errors

        # Access the response data
        
        logger.info(f"Now in function tiktok_get_user_info, getting resp {resp_json}")


        # Note: XCom data is now handled in the calling DAG task
        if context and 'ti' in context:
            logger.debug(f"Context available for username: {username}")
        else:
            logger.debug(f"Context not available for username: {username}")

        df = pd.DataFrame([resp_json["data"]])  # Wrap the dictionary in a list to create a single-row DataFrame

        return df

    except requests.exceptions.HTTPError as http_err:
        logger.info("TIKTOK request requests.exceptions.HTTPError")
        # Print detailed information about the error
        logger.info(f"HTTP error occurred: {http_err}", exc_info=True)
        logger.info(f"Status Code: {response.status_code}", exc_info=True)
        logger.info(f"Response Content: {response.text}", exc_info=True)
        logger.info(f"Response Headers: {response.headers}", exc_info=True)
        return None # Return None to avoid raising an exception and task failure
        
    except requests.exceptions.RequestException as err:
        logger.info("TIKTOK request requests.exceptions.RequestException", exc_info=True)
        logger.info(f"Other error occurred: {err}", exc_info=True)
        return None # Return None to avoid raising an exception


    except Exception as e:
        logger.info("TIKTOK request All Other Exception")
        logger.info(f"An unexpected error occurred: {e}", exc_info=True)
        return None # Return None to avoid raising an exception and task failure



def tiktok_get_video_comments(video_id):
    token_info = load_token_from_airflow()

    if is_token_expired(token_info):
         client_key = Variable.get("TIKTOK_CLIENT_KEY")
         client_secret = Variable.get("TIKTOK_CLIENT_SECRET")
         token_info = get_new_token(client_key, client_secret)

    url = 'https://open.tiktokapis.com/v2/research/video/comment/list/'
    headers = {
        "Authorization": token_info["token"],
        "Content-Type": "application/json"
    }
    params = {
        "fields": "id,text,video_id,parent_comment_id,like_count,reply_count,create_time"
    }

    all_comments = []
    cursor = 0
    max_count = 100  # Maximum allowed by API

    while True:
        try:
            body = {
                "video_id": video_id,
                "max_count": max_count,
                "cursor": cursor
            }

            logger.info(f"Fetching comments for video {video_id} with cursor {cursor}")
            response = requests.post(url, headers=headers, params=params, json=body)
            # Check for rate limit before raising other status errors
            if response.status_code == 429:
                logger.warning("Rate limit reached")
                raise requests.exceptions.HTTPError(
                    "429 Client Error: Too Many Requests", 
                    response=response
                )
            response.raise_for_status()
            resp = response.json()

            # Extract comments from response
            comments = resp.get("data", {}).get("comments", [])
            # if not comments:
            #     logger.info(f"No more comments found for video {video_id}")
            #     break

            # Add fetched time to each comment
            for comment in comments:
                structured_comment = {
                    "id": comment.get("id"),
                    "video_id": comment.get("video_id"),
                    "text": comment.get("text", ""),
                    "like_count": comment.get("like_count", 0),
                    "reply_count": comment.get("reply_count", 0),
                    "parent_comment_id": comment.get("parent_comment_id"),
                    "create_time": datetime.fromtimestamp(comment.get("create_time", 0))                             
                }

                all_comments.append(structured_comment)

            # Check if there are more comments
            has_more = resp.get("data", {}).get("has_more", False)
            if not has_more:
                logger.info(f"No more comments to fetch for video {video_id}")
                break

            # Update cursor for next batch
            cursor = resp.get("data", {}).get("cursor")
            if not cursor:
                logger.info(f"No cursor found for video {video_id}")
                break

            # API limit: only top 1000 comments can be retrieved
            if cursor >= 1000:
                logger.info(f"Reached 1000 comment limit for video {video_id}")
                break

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 429:
                # Let the calling function handle the rate limit
                raise
            logger.error(f"HTTP error occurred while fetching comments: {http_err}")
            return None
            
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Request error occurred while fetching comments: {req_err}")
            raise AirflowFailException(f"Failed to fetch comments: {req_err}")
            
        except Exception as e:
            logger.error(f"Unexpected error occurred while fetching comments: {e}")
            raise AirflowFailException(f"Failed to fetch comments: {e}")

    logger.info(f"Successfully fetched {len(all_comments)} comments for video {video_id}")
    return all_comments


def tiktok_get_user_video_info(username: str, **context):
    if context is None:
        context = {}

    # Load the token
    token_info = load_token_from_airflow()

    if is_token_expired(token_info):
        client_key = Variable.get("TIKTOK_CLIENT_KEY")
        client_secret = Variable.get("TIKTOK_CLIENT_SECRET")
        token_info = get_new_token(client_key, client_secret)

    logger.info(f"Now in function tiktok_get_user_video_info, fetching videos for {username}")
    url = 'https://open.tiktokapis.com/v2/research/video/query/'
    headers = {"Authorization": token_info["token"], "Content-Type": "application/json"}
    params = {"fields": "id, video_description, create_time, region_code, share_count, view_count, like_count, comment_count, music_id, hashtag_names, username, effect_ids, playlist_id,voice_to_text, is_stem_verified, video_duration, hashtag_info_list, video_mention_list, video_label, sticker_info_list, effect_info_list, video_tag"}

    start_date = datetime.strptime("20230101", "%Y%m%d")
    end_date = datetime.strptime("20231231", "%Y%m%d")
    date_ranges = generate_date_ranges(start_date, end_date)

    all_video_data = []

    for start, end in date_ranges:
        logger.info(f"Requesting videos for {username} from {start} to {end}")
        cursor = 0
        has_more = True

        while has_more:
            body = {
                "query": {
                    "and": [{"operation": "IN", "field_name": "username", "field_values": [username]}]
                },
                "max_count": 100,
                "start_date": start,
                "end_date": end,
                "cursor": cursor
            }

            try:
                response = requests.post(url, headers=headers, params=params, json=body, timeout=60)
                response.raise_for_status()
                resp = response.json()

                logger.info(f"Received response for {username}: {json.dumps(resp, indent=2)}")

                videos = resp.get("data", {}).get("videos", [])
                cursor = resp.get("data", {}).get("cursor", None)
                if cursor is None:
                    logger.warning(f"No cursor returned. Stopping pagination for {username} from {start} to {end}.")
                    break
                has_more = resp.get("data", {}).get("has_more", False)
                search_id = resp.get("data", {}).get("search_id", "")

                for video in videos:
                    video_id = video.get("id")
                    tiktok_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
                    oembed_data = {}
                    try:
                        oembed_response = requests.get("https://www.tiktok.com/oembed", params={"url": tiktok_url})
                        oembed_response.raise_for_status()
                        oembed_data = oembed_response.json()
                    except requests.exceptions.RequestException as oe:
                        logger.warning(f"oEmbed request failed for {tiktok_url}: {oe}")   
                    extracted_video = {
                        "search_id": search_id,
                        "username": username,
                        "video_id": video.get("id"),
                        "video_description": video.get("video_description"),
                        "create_time": datetime.fromtimestamp(video.get("create_time")).strftime('%Y-%m-%d'),
                        "region_code": video.get("region_code"),
                        "share_count": video.get("share_count"),
                        "view_count": video.get("view_count"),
                        "like_count": video.get("like_count"),
                        "comment_count": video.get("comment_count"),
                        "music_id": video.get("music_id"),
                        "hashtag_names": video.get("hashtag_names"),
                        "effect_ids": video.get("effect_ids"),
                        "playlist_id": video.get("playlist_id"),
                        "voice_to_text": video.get("voice_to_text"),
                        "is_stem_verified": video.get("is_stem_verified"),
                        "video_duration": video.get("video_duration"),
                        "hashtag_info_list": video.get("hashtag_info_list"),
                        "video_mention_list": video.get("video_mention_list"),
                        "video_label": video.get("video_label"),
                        "sticker_info_list": video.get("sticker_info_list"),
                        "effect_info_list": video.get("effect_info_list"), 
                        "video_tag": video.get("video_tag"),
                        "fetched_time": datetime.now(),
                        "video_title": oembed_data.get("title"),
                        "video_author_url": oembed_data.get("author_url"),
                        "video_thumbnail_url": oembed_data.get("thumbnail_url")
                    }
                    logger.info(f"Extracted video: {extracted_video}")
                    all_video_data.append(extracted_video)
                    logger.info(f"All video data: {all_video_data}")
                    
            except requests.exceptions.HTTPError as http_err:
                logger.error(f"HTTP error occurred: {http_err}", exc_info=True)
                raise
            except requests.exceptions.RequestException as err:
                logger.error(f"Other error occurred: {err}", exc_info=True)
                raise
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}", exc_info=True)
                raise

    df = pd.DataFrame(all_video_data)
    return df

