import requests
import logging
from datetime import datetime
from airflow.exceptions import AirflowFailException
import gridfs
from pymongo import MongoClient
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from airflow.sdk import Variable 
import pickle
import os

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

def save_thumbnail(image_url, video_id, channel_title):
    mongo_uri = Variable.get("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client["airflow_db"]
    fs = gridfs.GridFS(db)
    try:
        response = requests.get(image_url)
        response.raise_for_status()
        filename = f"{video_id}_{channel_title.replace(' ', '_')}_{image_url.split('/')[-1]}"
        return fs.put(response.content, filename=filename, video_id = video_id, channel_title = channel_title)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download image: {image_url}, Error: {e}")
        return None

def get_channels_statistics(channel_id):
    youtube_api_key = Variable.get("YOUTUBE_API_KEY")
    logger.debug(f"Fetching statistics for channel ID: {channel_id}")
    url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics,brandingSettings,topicDetails&id={channel_id}&key={youtube_api_key}'

    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        logger.debug(f"Received data for channel ID: {channel_id} - {data}")

    except requests.exceptions.HTTPError as http_err:
        raise Exception(f"HTTP error occurred: {http_err}") from http_err
        
    except requests.exceptions.RequestException as req_err:
          raise Exception(f"Request failed for channel ID: {channel_id} - {req_err}") from req_err
             
    if 'items' in data and len(data['items']) > 0:
        item = data['items'][0]
        stats = item.get('statistics', {})
        branding = item.get('brandingSettings', {}).get('channel', {})
        title = branding.get('title', 'Unknown')
        description = branding.get('description', 'Unknown')
        keywords = branding.get('keywords', [])
        country = branding.get('country', 'Unknown')
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
             'topic_categories': topic_categories
        }
    else:
        raise Exception(f"No items found for channel ID: {channel_id}")
        
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
            response.raise_for_status()
            data = response.json()
            
            for item in data.get('items', []):
                video_details[item['id']] = {
                    'statistics': item.get('statistics', {}),
                    'topicDetails': item.get('topicDetails', {}),
                    'snippet': item.get('snippet', {})
                }
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
            thumbnail_id = save_thumbnail(thumbnails, video_id, channelTitle)

            videos.append({
                          'video_title': video_title, 
                          'video_id': video_id, 
                          'published_at': published_at, 
                          'channel_id': channel_id, 
                          'video_description': video_description, 
                          'channel_title' : channelTitle,
                          'thumbnails': {'gridfs_id': thumbnail_id}       
                           })

        next_page_token = data.get('nextPageToken')
        if not next_page_token:
            logger.info("No more pages to fetch.")
            break

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
                    'tags': details['snippet'].get('tags', [])  
                    
                })
    logger.info(f"Total videos fetched: {len(videos)}")
    return videos



def get_top_level_comments(video_id):
    youtube_api_key = Variable.get("YOUTUBE_API_KEY")
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        'part': 'snippet',
        'videoId': video_id,
        'maxResults': 100, 
        'key': youtube_api_key
    }

    comments = []
    next_page_token = None

    logger.debug(f"Starting to fetch top-level comments for video_id: {video_id}")

    while True:
        if next_page_token:
            params['pageToken'] = next_page_token
        try:
            response = requests.get(url, params=params)
            response.raise_for_status() 

            data = response.json()
            logger.debug(f"Fetched {len(data.get('items', []))} comments for video_id: {video_id}")

            for item in data.get('items', []):
                top_comment = {
                    'comment_id': item['snippet']['topLevelComment']['id'],
                    'channel_id': item['snippet']['channelId'],
                    'video_id': item['snippet']['videoId'],
                    'canReply': item['snippet']['canReply'],
                    'totalReplyCount': item['snippet']['totalReplyCount'],
                    'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'authorDisplayName': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'authorProfileImageUrl': item['snippet']['topLevelComment']['snippet']['authorProfileImageUrl'],
                    'authorChannelUrl': item['snippet']['topLevelComment']['snippet']['authorChannelUrl'],
                    'canRate': item['snippet']['topLevelComment']['snippet']['canRate'],
                    'viewerRating': item['snippet']['topLevelComment']['snippet']['viewerRating'],
                    'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],
                    'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'updatedAt': item['snippet']['topLevelComment']['snippet']['updatedAt']         
                }
                comments.append(top_comment)

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                logger.info(f"Completed fetching comments for video_id: {video_id}")
                break
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise AirflowFailException(f"HTTP error while fetching comments for video_id {video_id}: {http_err}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise  AirflowFailException(f"Network error while fetching comments for video_id {video_id}: {e}")    

    logger.debug(f"Total comments fetched for video_id: {video_id}: {len(comments)}")     
    return comments

def get_replies(parent_id):
    youtube_api_key = Variable.get("YOUTUBE_API_KEY")
    url = "https://www.googleapis.com/youtube/v3/comments"
    params = {
        'part': 'snippet',
        'parentId': parent_id,
        'maxResults': 100,
        'key': youtube_api_key
    }

    replies = []
    next_page_token = None
    

    while True:
        if next_page_token:
            params['pageToken'] = next_page_token
        try:
            logger.debug(f"Starting to fetch replies for a comment id: {parent_id}")
            response = requests.get(url, params=params)
            response.raise_for_status() 
            data = response.json()

            for item in data.get('items', []):
                reply = {
                    'reply_id': item['id'],
                    'authorDisplayName': item['snippet']['authorDisplayName'],
                    'authorProfileImageUrl': item['snippet']['authorProfileImageUrl'],
                    'authorChannelUrl': item['snippet']['authorChannelUrl'],
                    'channel_id': item['snippet']['channelId'],
                    'text': item['snippet']['textDisplay'],
                    'parent_id': item['snippet']['parentId'],
                    'canRate': item['snippet']['canRate'],
                    'viewerRating': item['snippet']['viewerRating'],
                    'likeCount': item['snippet']['likeCount'],
                    'publishedAt': item['snippet']['publishedAt'],
                    'updatedAt': item['snippet']['updatedAt'],
                    'fetched_time': datetime.now()
                }                
                replies.append(reply)
            
                # nested_replies = get_replies(reply['reply_id'])
                # replies.extend(nested_replies)
            
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break
            

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise AirflowFailException(f"HTTP error while fetching replies for comment with comment_id {parent_id}: {http_err}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise  AirflowFailException(f"Network error while fetching replies for comment with comment_id: {parent_id}: {e}")    

    logger.debug(f"Replies were fetched for comment: {parent_id}")             
    return replies



def get_youtube_credentials():
    """
    Get or refresh OAuth 2.0 credentials
    """
    creds = None
    token_path = '/opt/airflow/config/credentials/token.pickle'
    
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.error(f"Failed to refresh token: {e}")
                raise AirflowFailException("Token refresh failed")
        else:
            raise AirflowFailException(
                "No valid credentials found. Please run the authentication setup script."
            )
            
    return creds

# def get_captions(video_id):
#     """
#     Fetch captions using OAuth authentication
#     """
#     creds = get_youtube_credentials()
    
#     # First, get the caption tracks available for the video
#     captions_url = "https://www.googleapis.com/youtube/v3/captions"
#     params = {
#         'part': 'snippet',
#         'videoId': video_id     
#     }

#     headers = {
#         'Authorization': f'Bearer {creds.token}',
#         'Accept': 'application/json'
#     }

#     captions_list = []
    
#     try:
#         # Get list of available captions
#         response = requests.get(captions_url, params=params, headers=headers)
#         response.raise_for_status()
#         data = response.json() 

#         logger.info(f"Available captions for video {video_id}: {data}") 

#         for item in data.get('items', []):
#             caption_info = {
#                 'caption_id': item['id'],
#                 'video_id': video_id,
#                 'language': item['snippet']['language'],
#                 'language_name': item['snippet'].get('name', ''),
#                 'track_kind': item['snippet']['trackKind'],
#                 'is_auto': item['snippet']['trackKind'] == 'ASR',
#                 'is_draft': item['snippet'].get('isDraft', False),
#                 'fetched_time': datetime.now()
#             }
            
#             # Get the actual caption content
#             caption_download_url = f"{captions_url}/{item['id']}"
#             download_params = {
#                 'tfmt': 'srt'
                
                  
#             }
#             download_headers = {
#                 'Authorization': f'Bearer {creds.token}',
#                 'Accept': 'application/octet-stream'
#             }
            
#             try:
#                 caption_response = requests.get(
#                     caption_download_url, 
#                     params=download_params,
#                     headers=download_headers,
#                     stream=True 
#                 )
#                 logger.info(f"Caption download response status: {caption_response.status_code}")
#                 logger.info(f"Caption download response headers: {caption_response.headers}")
#                 if caption_response.status_code == 403:
#                     logger.error(f"Full error response: {caption_response.text}")

#                 caption_response.raise_for_status()
#                 caption_info['caption_content'] = caption_response.content.decode('utf-8')
                
#             except requests.exceptions.RequestException as e:
#                 logger.error(f"Failed to download caption content: {e}")
#                 caption_info['caption_content'] = None
                
#             captions_list.append(caption_info)
            
#         return captions_list
        
#     except Exception as e:
#         logger.error(f"Failed to fetch captions: {e}")
#         raise AirflowFailException(f"Caption fetching failed: {e}")
  

def get_captions(video_id):
    """
    Fetch captions following YouTube API documentation
    """
    creds = get_youtube_credentials()
    
    # Step 1: Get caption track ID
    list_url = "https://www.googleapis.com/youtube/v3/captions"
    list_params = {
        'part': 'snippet',
        'videoId': video_id
    }
    headers = {
        'Authorization': f'Bearer {creds.token}'
    }
    
    try:
        # Get list of available captions
        response = requests.get(list_url, params=list_params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Find English caption or auto-generated caption
        caption_id = None
        for item in data.get('items', []):
            if item['snippet']['language'] == 'en':
                caption_id = item['id']
                break
        
        if not caption_id:
            logger.info(f"No English captions found for video {video_id}")
            return None
            
        # Step 2: Download the caption using the ID
        download_url = f"https://www.googleapis.com/youtube/v3/captions/{caption_id}"
        download_params = {
            'tfmt': 'srt'  # Request in SubRip format
        }
        download_headers = {
            'Authorization': f'Bearer {creds.token}',
            'Accept': 'application/octet-stream'  # As specified in docs
        }
        
        caption_response = requests.get(
            download_url,
            params=download_params,
            headers=download_headers
        )
        caption_response.raise_for_status()
        
        return {
            'caption_id': caption_id,
            'video_id': video_id,
            'language': 'en',
            'caption_content': caption_response.content.decode('utf-8'),
            'fetched_time': datetime.now()
        }
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            logger.error(f"Permission denied for video {video_id}: {e.response.text}")
        elif e.response.status_code == 404:
            logger.error(f"Caption not found for video {video_id}")
        else:
            logger.error(f"HTTP error occurred: {e}")
        return None
        
    except Exception as e:
        logger.error(f"Failed to fetch captions: {e}")
        return None