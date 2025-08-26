var results = [];
db.youtube_video_replies.aggregate([
    // First group by parent comment to get per-comment stats
    {
      $group: {
        _id: {
          channel_id: "$channel_id",
          parent_id: "$parent_id"
        },
        replies_per_comment: { $sum: 1 },  // Count replies per comment
        total_likes: { $sum: { $toLong: "$likeCount" } },  // Sum likes on replies
        unique_repliers: { $addToSet: "$authorChannelUrl" }  // Count unique repliers
      }
    },
    
    // Then group by channel to get overall stats
    {
      $group: {
        _id: "$_id.channel_id",
        total_comments_with_replies: { $sum: 1 },  // Count comments that have replies
        total_replies: { $sum: "$replies_per_comment" },  // Total number of replies
        total_likes_on_replies: { $sum: "$total_likes" },  // Total likes on all replies
        
        // Calculate unique repliers per channel
        unique_repliers_count: { 
          $addToSet: "$unique_repliers" 
        },
        
        // Calculate averages per comment
        avg_replies_per_comment: { $avg: "$replies_per_comment" },
        avg_likes_per_reply: { 
          $avg: { 
            $divide: ["$total_likes", "$replies_per_comment"] 
          }
        },
        
        // Calculate min/max per comment
        min_replies_per_comment: { $min: "$replies_per_comment" },
        max_replies_per_comment: { $max: "$replies_per_comment" },
        min_likes_per_comment_replies: { $min: "$total_likes" },
        max_likes_per_comment_replies: { $max: "$total_likes" }
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
    
    // Unwind channel_info array
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
        replies_stats: {
          total_comments_with_replies: "$total_comments_with_replies",
          total_replies: "$total_replies",
          total_likes_on_replies: "$total_likes_on_replies",
          unique_repliers: { 
            $size: { 
              $reduce: {
                input: "$unique_repliers_count",
                initialValue: [],
                in: { $setUnion: ["$$value", "$$this"] }
              }
            }
          },
          
          per_comment_metrics: {
            replies: {
              avg: { $round: ["$avg_replies_per_comment", 2] },
              min: "$min_replies_per_comment",
              max: "$max_replies_per_comment"
            },
            likes: {
              avg: { $round: ["$avg_likes_per_reply", 2] },
              min: "$min_likes_per_comment_replies",
              max: "$max_likes_per_comment_replies"
            }
          },
          
          engagement_metrics: {
            avg_likes_per_reply: {
              $round: [{
                $cond: [
                  { $eq: ["$total_replies", 0] },
                  0,
                  { $divide: ["$total_likes_on_replies", "$total_replies"] }
                ]
              }, 2]
            },
            replies_per_comment_ratio: {
              $round: [{
                $cond: [
                  { $eq: ["$total_comments_with_replies", 0] },
                  0,
                  { $divide: ["$total_replies", "$total_comments_with_replies"] }
                ]
              }, 2]
            }
          }
        }
      }
    },
    
    // Sort by total replies (highest first)
    { $sort: { "replies_stats.total_replies": -1 } }
    
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
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/video_replies_stats.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath); 