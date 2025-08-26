var results = [];
db.youtube_video_comments.aggregate([
    // First group by video to get per-video stats
    {
      $group: {
        _id: {
          channel_id: "$channel_id",
          video_id: "$video_id"
        },
        video_comments: { $sum: 1 },  // Count comments per video
        video_likes: { $sum: { $toLong: "$likeCount" } },  // Sum likes per video
        video_replies: { $sum: { $toLong: "$totalReplyCount" } }  // Sum replies per video
      }
    },
    
    // Then group by channel to get overall stats
    {
      $group: {
        _id: "$_id.channel_id",
        total_videos: { $sum: 1 },  // Count total videos
        total_comments: { $sum: "$video_comments" },
        total_likes: { $sum: "$video_likes" },
        total_replies: { $sum: "$video_replies" },
        
        // Calculate averages per video
        avg_comments_per_video: { $avg: "$video_comments" },
        avg_likes_per_video: { $avg: "$video_likes" },
        avg_replies_per_video: { $avg: "$video_replies" },
        
        // Calculate min/max per video
        min_comments_per_video: { $min: "$video_comments" },
        max_comments_per_video: { $max: "$video_comments" },
        min_likes_per_video: { $min: "$video_likes" },
        max_likes_per_video: { $max: "$video_likes" },
        min_replies_per_video: { $min: "$video_replies" },
        max_replies_per_video: { $max: "$video_replies" }
      }
    },
    
    // Lookup channel information from youtube_channel_stats
    {
      $lookup: {
        from: "youtube_channel_stats",
        localField: "_id",
        foreignField: "channel_id",
        as: "channel_info"
      }
    },
    
    // Unwind channel_info array (converts array to object)
    {
      $unwind: {
        path: "$channel_info",
        preserveNullAndEmptyArrays: true
      }
    },
    
    // Format the output
    {
      $project: {
        _id: 0,
        channel_id: "$_id",
        channel_title: { $ifNull: ["$channel_info.title", "Unknown"] },
        channel_stats: {
          subscriber_count: { $ifNull: ["$channel_info.subscriber_count", 0] },
          view_count: { $ifNull: ["$channel_info.view_count", 0] },
          video_count: { $ifNull: ["$channel_info.video_count", 0] }
        },
        comments_stats: {
          total_videos_with_comments: "$total_videos",
          total_comments: "$total_comments",
          total_likes: "$total_likes",
          total_replies: "$total_replies",
          
          comments_per_video: {
            avg: { $round: ["$avg_comments_per_video", 2] },
            min: "$min_comments_per_video",
            max: "$max_comments_per_video"
          },
          likes_per_video: {
            avg: { $round: ["$avg_likes_per_video", 2] },
            min: "$min_likes_per_video",
            max: "$max_likes_per_video"
          },
          replies_per_video: {
            avg: { $round: ["$avg_replies_per_video", 2] },
            min: "$min_replies_per_video",
            max: "$max_replies_per_video"
          },
          
          engagement_metrics: {
            likes_per_comment: {
              $round: [{
                $cond: [
                  { $eq: ["$total_comments", 0] },
                  0,
                  { $divide: ["$total_likes", "$total_comments"] }
                ]
              }, 2]
            },
            replies_per_comment: {
              $round: [{
                $cond: [
                  { $eq: ["$total_comments", 0] },
                  0,
                  { $divide: ["$total_replies", "$total_comments"] }
                ]
              }, 2]
            }
          }
        }
      }
    },
    
    // Sort by total comments (highest first)
    { $sort: { "comments_stats.total_comments": -1 } }
    
], { allowDiskUse: true }  // Allow using disk for large operations
).forEach(function(doc) {
    // Convert any Long numbers in the nested structure
    const convertLongNumbers = (obj) => {
        if (obj === null || typeof obj !== 'object') return obj;
        
        if (obj.hasOwnProperty('low') && obj.hasOwnProperty('high') && obj.hasOwnProperty('unsigned')) {
            return NumberLong(obj).toNumber();
        }
        
        if (Array.isArray(obj)) {
            return obj.map(convertLongNumbers);
        }
        
        const converted = {};
        for (let key in obj) {
            converted[key] = convertLongNumbers(obj[key]);
        }
        return converted;
    };
    
    results.push(convertLongNumbers(doc));
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/video_comments_stats.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath); 