import requests  # pyright: ignore[reportMissingModuleSource]
import logging
from datetime import datetime
from airflow.exceptions import AirflowFailException  # pyright: ignore[reportMissingImports]
import gridfs  # pyright: ignore[reportMissingImports]
from pymongo import MongoClient  # pyright: ignore[reportMissingImports]
from google.oauth2.credentials import Credentials  # pyright: ignore[reportMissingImports]
from google_auth_oauthlib.flow import Flow  # pyright: ignore[reportMissingImports]
from airflow.sdk import Variable   # pyright: ignore[reportMissingImports]
import pickle
import os
import time
from pymongo.errors import BulkWriteError, OperationFailure  # pyright: ignore[reportMissingImports]
from api_rate_limits import fail_if_youtube_quota, youtube_quota_reason, PlatformRateLimitError

# Set up logging - log to airflow logs & console
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.DEBUG)  # Set the log level
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
if not logger.hasHandlers():  # Avoid duplicate handlers
    logger.addHandler(stream_handler)


# Variables will be accessed when needed inside functions
# YOUTUBE_API_KEY = Variable.get("YOUTUBE_API_KEY")
# MONGO_URI = Variable.get("MONGO_URI")

# def save_thumbnail(image_url, video_id, channel_title):
#     mongo_uri = Variable.get("MONGO_URI")
#     client = MongoClient(mongo_uri)
#     db = client["rbl"]
#     fs = gridfs.GridFS(db)
#     try:
#         response = requests.get(image_url)
#         response.raise_for_status()
#         filename = f"{video_id}_{channel_title.replace(' ', '_')}_{image_url.split('/')[-1]}"
#         return fs.put(response.content, filename=filename, video_id = video_id, channel_title = channel_title)
#     except requests.exceptions.RequestException as e:
#         logger.error(f"Failed to download image: {image_url}, Error: {e}")
#         return None

def get_channels_statistics(channel_id):
    youtube_api_key = Variable.get("YOUTUBE_API_KEY")
    logger.debug(f"Fetching statistics for channel ID: {channel_id}")
    url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet,brandingSettings,topicDetails&id={channel_id}&key={youtube_api_key}'

    try:
        response = requests.get(url)
        fail_if_youtube_quota(response, context=f"fetching channel stats for {channel_id}")
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Received data for channel ID: {channel_id} - {data}")

    except PlatformRateLimitError:
        raise
    except requests.exceptions.HTTPError as http_err:
        logger.warning(f"HTTP error occurred: {http_err}")
        return None
    except requests.exceptions.RequestException as req_err:
          logger.warning(f"Request failed for channel ID: {channel_id} - {req_err}")
          return None
    except Exception as e:
          logger.warning(f"Unexpected error occurred: {e}")
          return None
              
    if 'items' in data and len(data['items']) > 0:
        item = data['items'][0]
        stats = item.get('statistics', {})
        snippet = item.get('snippet', {})
        branding = item.get('brandingSettings', {}).get('channel', {})
        title = branding.get('title', 'Unknown')
        description = branding.get('description', 'Unknown')
        keywords = branding.get('keywords', [])
        country = branding.get('country', 'Unknown')
        images = item.get('brandingSettings', {}).get('image', {})
        topic_categories = item.get('topicDetails', {}).get('topicCategories', [])
        logger.info(f"Statistics fetched successfully for channel ID: {channel_id}")
        return {
             'channel_id': channel_id,
             'title': title,
             'view_count': stats.get('viewCount', '0'),
             'subscriber_count': stats.get('subscriberCount', '0'),
             'video_count': stats.get('videoCount', '0'),
             'hidden_subscriber_count': stats.get('hiddenSubscriberCount', False),
             'description': description,
             'keywords': keywords,
             'country': country,
             'topic_categories': topic_categories,
             'banner_external_url': images.get('bannerExternalUrl'),
             'thumbnail_url': snippet.get('thumbnails', {}).get('high', {}).get('url')
        }
    else:
        logger.warning(f"No items found for channel ID: {channel_id}")
        return None
        
def get_video_details(video_ids):   
    """
    Fetch additional details for videos using the videos endpoint
    """
    # YouTube API limits: max 50 video IDs per request
    video_ids_chunks = [video_ids[i:i + 50] for i in range(0, len(video_ids), 50)]
    video_details = {}
    
    youtube_api_key = Variable.get("YOUTUBE_API_KEY")
    for chunk in video_ids_chunks:
        url = f'https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet,topicDetails&id={",".join(chunk)}&key={youtube_api_key}'
        try:
            response = requests.get(url)
            fail_if_youtube_quota(response, context="fetching video details")
            response.raise_for_status()
            data = response.json()

            for item in data.get('items', []):
                video_details[item['id']] = {
                    'statistics': item.get('statistics', {}),
                    'topicDetails': item.get('topicDetails', {}),
                    'snippet': item.get('snippet', {})
                }
        except PlatformRateLimitError:
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching video details: {e}")
            raise AirflowFailException(f"Failed to fetch video details: {e}")
            
    return video_details

def get_videos_by_date(channel_id, start_date, end_date):
    youtube_api_key = Variable.get("YOUTUBE_API_KEY")
    base_url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&type=video&order=date&maxResults=50&key={youtube_api_key}'
    videos = []
    video_ids = []
    next_page_token = None
    logger.info(f"Fetching videos for channel_id: {channel_id} from {start_date} to {end_date}")

    while True:
     url = base_url + f'&publishedAfter={start_date}&publishedBefore={end_date}'
     if next_page_token:
            url += f'&pageToken={next_page_token}'
     try:
        response = requests.get(url)
        fail_if_youtube_quota(
            response,
            context=f"fetching videos for channel {channel_id}",
        )
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Fetched {len(data.get('items', []))} videos from page.")

        for item in data.get('items', []):
            video_id = item['id']['videoId']
            video_ids.append(video_id)
            
            video_title = item['snippet']['title']
            published_at = item['snippet']['publishedAt']
            video_description = item['snippet']['description']
            channelTitle = item['snippet']['channelTitle']
            thumbnails = item['snippet']['thumbnails']['high']['url']
             
            # Save thumbnail and get GridFS ID
            # thumbnail_id = save_thumbnail(thumbnails, video_id, channelTitle)

            videos.append({
                          'video_title': video_title, 
                          'video_id': video_id, 
                          'published_at': published_at, 
                          'channel_id': channel_id, 
                          'video_description': video_description, 
                          'channel_title' : channelTitle,
                          'thumbnail_url':  thumbnails     
                           })

        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            logger.info("No more pages to fetch.")
            break

     except PlatformRateLimitError:
            raise
     except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise AirflowFailException(f"HTTP error fetching videos for channel {channel_id}: {http_err}")

     except requests.exceptions.RequestException as req_err:
            logger.error(f"Request failed: {req_err}")
            raise AirflowFailException(f"Request failed for channel {channel_id}: {req_err}")

     except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise AirflowFailException(f"Unexpected error fetching videos for channel {channel_id}: {e}")   

    if video_ids:
        video_details = get_video_details(video_ids)
        
        # Merge the details into the videos list
        for video in videos:
            video_id = video['video_id']
            if video_id in video_details:
                details = video_details[video_id]
                video.update({
                    'view_count': details['statistics'].get('viewCount', '0'),
                    'like_count': details['statistics'].get('likeCount', '0'),
                    'comment_count': details['statistics'].get('commentCount', '0'),
                    'topic_categories': details['topicDetails'].get('topicCategories', []),
                    'tags': details['snippet'].get('tags', []),
                    'defaultAudioLanguage': details['snippet'].get('defaultAudioLanguage', None),
                    'defaultLanguage': details['snippet'].get('defaultLanguage', None),
                    
                })
    logger.info(f"Total videos fetched: {len(videos)}")
    return videos

def safe_bulk_write(collection, operations, session=None, max_retries=10):
    retry = 0

    while retry < max_retries:
        try:
            return collection.bulk_write(
                operations,
                ordered=False,
                session=session
            )

        except BulkWriteError as bwe:
            write_errors = bwe.details.get("writeErrors", [])
            # Check for 16500 / 429 CosmosDB throttling
            throttled = any(err.get("code") == 16500 for err in write_errors)

            if throttled:
                wait = 0.5 * (2 ** retry)
                logger.warning(f"[safe_bulk_write] CosmosDB 429/16500 throttling. Retrying in {wait:.1f}s...")
                time.sleep(wait)
                retry += 1
                continue

            raise  # other errors should not be retried

        except OperationFailure as e:
            # CosmosDB 16500 throttling
            if e.code == 16500:
                wait = 0.5 * (2 ** retry)
                logger.warning(f"[safe_bulk_write] OperationFailure 16500. Retrying in {wait:.1f}s...")
                time.sleep(wait)
                retry += 1
                continue

            raise

    raise Exception(f"safe_bulk_write failed after {max_retries} retries.")

def get_top_level_comments(video_id, order_by='relevance'):
    youtube_api_key = Variable.get("YOUTUBE_API_KEY")
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        'part': 'snippet',
        'videoId': video_id,
        'maxResults': 100, 
        'textFormat': 'plainText',
        'key': youtube_api_key,
        'order': order_by
    }

    comments = []
    next_page_token = None
    max_comments = 1000 

    logger.debug(f"Starting to fetch top-level comments for video_id: {video_id}")

    while True:
        if next_page_token:
            params['pageToken'] = next_page_token
        try:
            response = requests.get(url, params=params)
            fail_if_youtube_quota(
                response,
                context=f"fetching comments for video_id {video_id}",
            )
            response.raise_for_status()

            data = response.json()
            logger.debug(f"Fetched {len(data.get('items', []))} comments for video_id: {video_id}")

            for item in data.get('items', []):
               
                top_comment = {
                    'comment_id': item['snippet']['topLevelComment']['id'],
                    'channel_id': item['snippet']['channelId'],
                    'video_id': item['snippet']['videoId'],
                    'parent_id': None,
                    'canReply': item['snippet']['canReply'],
                    'totalReplyCount': item['snippet']['totalReplyCount'],
                    'textDisplay': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'textOriginal': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'authorDisplayName': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'authorProfileImageUrl': item['snippet']['topLevelComment']['snippet']['authorProfileImageUrl'],
                    'authorChannelUrl': item['snippet']['topLevelComment']['snippet']['authorChannelUrl'],
                    'authorChannelId': item['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                    'canRate': item['snippet']['topLevelComment']['snippet']['canRate'],
                    'viewerRating': item['snippet']['topLevelComment']['snippet']['viewerRating'],     
                    'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],
                    'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'updatedAt': item['snippet']['topLevelComment']['snippet']['updatedAt']         
                }
                comments.append(top_comment)
                if len(comments) >= max_comments:
                    logger.info(f"Reached maximum {max_comments} comments limit for video_id: {video_id}")
                    return comments
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                logger.info(f"Completed fetching comments for video_id: {video_id}")
                break
        except PlatformRateLimitError:
            raise
        except requests.exceptions.HTTPError as e:
            quota_reason = youtube_quota_reason(e.response)
            if quota_reason:
                raise PlatformRateLimitError(
                    f"YouTube API quota/rate limit hit ({quota_reason}) while fetching "
                    f"comments for video_id {video_id}. Daily quota resets at midnight "
                    "Pacific Time. Re-run after reset; completed records remain checkpointed in Mongo."
                ) from e
            if e.response.status_code == 403:
                try:
                    error_json = e.response.json()
                    reason= error_json.get('error', {}).get('errors', [{}])[0].get('reason')
                    if reason == 'commentsDisabled':
                        logger.info(f"Comments are disabled for video_id: {video_id}")
                        return {"comments_disabled": True}
                    else:
                        logger.error(f"HTTP error occurred: {reason}")
                        raise AirflowFailException(f"HTTP error while fetching comments for video_id {video_id}: {e}")
                except Exception as e:
                    logger.error(f"Error parsing error response: {e}")
                    raise AirflowFailException(f"Error parsing error response for video_id {video_id}: {e}")
            else:
                logger.error(f"HTTP error occurred: {e}")
                raise AirflowFailException(f"HTTP error while fetching comments for video_id {video_id}: {e}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise  AirflowFailException(f"Network error while fetching comments for video_id {video_id}: {e}")    

    logger.debug(f"Total comments fetched for video_id: {video_id}: {len(comments)}")     
    return comments




