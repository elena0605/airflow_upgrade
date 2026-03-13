# Data Storage Summary Report

## Overview
This report summarizes all data types and structures stored in MongoDB and Neo4j databases across the Airflow DAGs in this project. The project processes data from TikTok and YouTube platforms, storing both raw data in MongoDB and transformed graph data in Neo4j.

## Database Configuration
- **Development Environment**: `airflow_db` (MongoDB), `neo4j` (Neo4j)
- **Production Environment**: `rbl` (MongoDB), `neo4j` (Neo4j)

---

## MongoDB Collections

### TikTok Data

#### 1. tiktok_user_info
**Purpose**: Stores TikTok user profile information
**Key Fields**:
- `username` (String, Unique) - TikTok username
- `display_name` (String) - User's display name
- `bio_description` (String) - User's bio text
- `bio_url` (String) - URL in bio
- `avatar_url` (String) - Profile picture URL
- `follower_count` (Number) - Number of followers
- `following_count` (Number) - Number of users followed
- `likes_count` (Number) - Total likes received
- `video_count` (Number) - Number of videos posted
- `is_verified` (Boolean) - Verification status
- `timestamp` (DateTime) - Data fetch timestamp
- `transformed_to_neo4j` (Boolean) - Processing flag

#### 2. tiktok_user_video
**Purpose**: Stores TikTok video metadata and statistics
**Key Fields**:
- `video_id` (String, Unique) - Unique video identifier
- `username` (String) - Video creator's username
- `search_id` (String) - Search session identifier
- `video_description` (String) - Video description text
- `create_time` (String) - Video creation date
- `region_code` (String) - Geographic region
- `share_count` (Number) - Number of shares
- `view_count` (Number) - Number of views
- `like_count` (Number) - Number of likes
- `comment_count` (Number) - Number of comments
- `music_id` (String) - Associated music track ID
- `hashtag_names` (Array) - List of hashtags
- `hashtag_info_list` (Array) - Detailed hashtag information
- `video_mention_list` (Array) - User mentions in video
- `video_label` (Object) - Video classification labels
- `video_tag` (Object) - Video tags and categories
- `sticker_info_list` (Array) - Stickers used in video
- `effect_info_list` (Array) - Video effects applied
- `voice_to_text` (String) - Transcribed audio content
- `is_stem_verified` (Boolean) - Audio verification status
- `video_duration` (Number) - Video length in seconds
- `video_title` (String) - Video title from oEmbed
- `video_author_url` (String) - Author profile URL
- `video_thumbnail_url` (String) - Thumbnail image URL
- `fetched_time` (DateTime) - Data fetch timestamp
- `transformed_to_neo4j` (Boolean) - Processing flag
- `comments_fetched` (Boolean) - Comments processing flag
- `comments_fetched_at` (DateTime) - Comments fetch timestamp
- `comments_count` (Number) - Number of comments fetched

#### 3. tiktok_video_comments
**Purpose**: Stores TikTok video comments and replies
**Key Fields**:
- `id` (String, Unique) - Comment unique identifier
- `video_id` (String) - Associated video ID
- `username` (String) - Comment author's username
- `text` (String) - Comment text content
- `like_count` (Number) - Comment likes
- `reply_count` (Number) - Number of replies
- `parent_comment_id` (String) - Parent comment ID (for replies)
- `create_time` (DateTime) - Comment creation time
- `fetched_at` (DateTime) - Data fetch timestamp
- `transformed_to_neo4j` (Boolean) - Processing flag

### YouTube Data

#### 4. youtube_channel_stats
**Purpose**: Stores YouTube channel statistics and metadata
**Key Fields**:
- `channel_id` (String, Unique) - YouTube channel identifier
- `username` (String) - Channel username
- `title` (String) - Channel title
- `view_count` (String) - Total channel views
- `subscriber_count` (String) - Number of subscribers
- `video_count` (String) - Number of videos
- `hidden_subscriber_count` (Boolean) - Subscriber privacy setting
- `description` (String) - Channel description
- `keywords` (Array) - Channel keywords
- `country` (String) - Channel country
- `topic_categories` (Array) - YouTube topic categories
- `timestamp` (DateTime) - Data fetch timestamp
- `transformed_to_neo4j` (Boolean) - Processing flag

#### 5. youtube_channel_videos
**Purpose**: Stores YouTube video metadata and statistics
**Key Fields**:
- `video_id` (String, Unique) - YouTube video identifier
- `video_title` (String) - Video title
- `published_at` (String) - Publication date
- `channel_id` (String) - Associated channel ID
- `video_description` (String) - Video description
- `channel_title` (String) - Channel name
- `thumbnails` (Object) - Thumbnail information
  - `gridfs_id` (String) - GridFS file reference
- `view_count` (Number) - Number of views
- `like_count` (Number) - Number of likes
- `comment_count` (Number) - Number of comments
- `topic_categories` (Array) - Video topic categories
- `tags` (Array) - Video tags
- `timestamp` (DateTime) - Data fetch timestamp
- `transformed_to_neo4j` (Boolean) - Processing flag
- `comments_fetched` (Boolean) - Comments processing flag
- `comments_fetched_at` (DateTime) - Comments fetch timestamp
- `comments_count` (Number) - Number of comments fetched

#### 6. youtube_video_comments
**Purpose**: Stores YouTube video top-level comments
**Key Fields**:
- `comment_id` (String, Unique) - Comment unique identifier
- `channel_id` (String) - Associated channel ID
- `video_id` (String) - Associated video ID
- `canReply` (Boolean) - Reply permission
- `totalReplyCount` (Number) - Number of replies
- `text` (String) - Comment text content
- `authorDisplayName` (String) - Comment author name
- `authorProfileImageUrl` (String) - Author profile image
- `authorChannelUrl` (String) - Author channel URL
- `canRate` (Boolean) - Rating permission
- `viewerRating` (String) - Viewer rating
- `likeCount` (Number) - Comment likes
- `publishedAt` (String) - Publication date
- `updatedAt` (String) - Last update date
- `fetched_at` (DateTime) - Data fetch timestamp
- `transformed_to_neo4j` (Boolean) - Processing flag
- `replies_fetched` (Boolean) - Replies processing flag
- `replies_fetched_at` (DateTime) - Replies fetch timestamp
- `replies_count` (Number) - Number of replies fetched

#### 7. youtube_video_replies
**Purpose**: Stores YouTube comment replies
**Key Fields**:
- `reply_id` (String, Unique) - Reply unique identifier
- `authorDisplayName` (String) - Reply author name
- `authorProfileImageUrl` (String) - Author profile image
- `authorChannelUrl` (String) - Author channel URL
- `channel_id` (String) - Associated channel ID
- `text` (String) - Reply text content
- `parent_id` (String) - Parent comment ID
- `canRate` (Boolean) - Rating permission
- `viewerRating` (String) - Viewer rating
- `likeCount` (Number) - Reply likes
- `publishedAt` (String) - Publication date
- `updatedAt` (String) - Last update date
- `fetched_time` (DateTime) - Data fetch timestamp
- `transformed_to_neo4j` (Boolean) - Processing flag

#### 8. youtube_video_captions
**Purpose**: Stores YouTube video captions/subtitles
**Key Fields**:
- `caption_id` (String) - Caption track identifier
- `video_id` (String) - Associated video ID
- `language` (String) - Caption language
- `language_name` (String) - Language display name
- `track_kind` (String) - Caption type (manual/auto)
- `is_auto` (Boolean) - Auto-generated flag
- `is_draft` (Boolean) - Draft status
- `caption_content` (String) - Full caption text
- `fetched_time` (DateTime) - Data fetch timestamp

---

## Neo4j Graph Database

### Node Types

#### TikTok Nodes

1. **TikTokUser**
   - Properties: `username`, `display_name`, `bio_description`, `bio_url`, `avatar_url`, `follower_count`, `following_count`, `likes_count`, `video_count`, `is_verified`

2. **TikTokVideo**
   - Properties: `video_id`, `video_description`, `create_time`, `region_code`, `share_count`, `view_count`, `like_count`, `comment_count`, `music_id`, `voice_to_text`, `is_stem_verified`, `video_duration`, `video_title`, `video_author_url`, `video_thumbnail_url`, `video_mention_list`, `video_label_content`, `video_tag_type`, `search_id`, `username`

3. **TikTokComment**
   - Properties: `comment_id`, `text`, `like_count`, `reply_count`, `create_time`, `username`, `video_id`, `parent_comment_id`

4. **Hashtag**
   - Properties: `id`, `name`, `description`

5. **Sticker**
   - Properties: `id`, `name`

#### YouTube Nodes

1. **YouTubeChannel**
   - Properties: `channel_id`, `title`, `view_count`, `subscriber_count`, `video_count`, `hidden_subscriber_count`, `description`, `keywords`, `country`, `topic_categories`, `username`

2. **YouTubeVideo**
   - Properties: `video_id`, `video_title`, `published_at`, `channel_id`, `video_description`, `channel_title`, `thumbnail_gridfs_id`, `view_count`, `like_count`, `comment_count`, `topic_categories`, `tags`

3. **YouTubeVideoComment**
   - Properties: `comment_id`, `channel_id`, `video_id`, `canReply`, `totalReplyCount`, `text`, `authorDisplayName`, `authorProfileImageUrl`, `authorChannelUrl`, `canRate`, `viewerRating`, `likeCount`, `publishedAt`, `updatedAt`

4. **YouTubeVideoReply**
   - Properties: `reply_id`, `channel_id`, `parent_id`, `text`, `authorDisplayName`, `authorProfileImageUrl`, `authorChannelUrl`, `canRate`, `viewerRating`, `likeCount`, `publishedAt`, `updatedAt`, `last_updated`

5. **YouTubeVideoCaption**
   - Properties: `caption_id`, `video_id`, `language`, `language_name`, `track_kind`, `is_auto`, `is_draft`, `caption_content`, `fetched_time`

6. **Tag**
   - Properties: `name`

### Relationship Types

#### TikTok Relationships
- `PUBLISHED_ON_TIKTOK` - TikTokUser → TikTokVideo
- `HAS_HASHTAG` - TikTokVideo → Hashtag
- `HAS_STICKER` - TikTokVideo → Sticker
- `POSTED_ON_TIKTOK_VIDEO` - TikTokComment → TikTokVideo
- `REPLIED_TO_TIKTOK_COMMENT` - TikTokComment → TikTokComment

#### YouTube Relationships
- `PUBLISHED_ON_YOUTUBE` - YouTubeChannel → YouTubeVideo
- `HAS_TAG` - YouTubeVideo → Tag
- `COMMENT_ON_YOUTUBE_VIDEO` - YouTubeVideoComment → YouTubeVideo
- `REPLY_TO_YOUTUBE_COMMENT` - YouTubeVideoReply → YouTubeVideoComment
- `CAPTIONOFVIDEO` - YouTubeVideoCaption → YouTubeVideo

---

## Data Processing Flow

1. **Data Ingestion**: Raw data fetched from TikTok/YouTube APIs
2. **MongoDB Storage**: Raw data stored with processing flags
3. **Neo4j Transformation**: Data transformed into graph structure
4. **Relationship Creation**: Connections established between entities
5. **Processing Flags**: `transformed_to_neo4j` flag tracks processing status

---

## Key Features

- **Dual Storage**: Raw data in MongoDB, graph data in Neo4j
- **Environment Support**: Development and production configurations
- **Processing Tracking**: Flags prevent duplicate processing
- **Error Handling**: Robust error handling and logging
- **Scalability**: Bulk operations and pagination support
- **Data Integrity**: Unique indexes and duplicate handling

---

## Data Volume Estimates

Based on the DAG configurations:
- **TikTok**: Up to 1000 comments per video, 100 videos per user
- **YouTube**: Up to 100 comments per video, 50 videos per channel
- **Captions**: Full text content for video accessibility
- **Metadata**: Rich metadata for analytics and insights

This comprehensive data structure enables complex social media analytics, influencer tracking, content analysis, and relationship mapping across platforms.
