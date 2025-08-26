var results = []; 
db.youtube_channel_videos.aggregate([
    {
      $facet: {
        "totalDocuments": [
          { $count: "count" }
        ],
        "fieldAnalysis": [
          {
            $project: {
              // Basic fields
              hasVideoTitle: { $cond: [{ $and: [{ $ne: ["$video_title", null] }, { $ne: ["$video_title", ""] }] }, 1, 0] },
              hasVideoId: { $cond: [{ $and: [{ $ne: ["$video_id", null] }, { $ne: ["$video_id", ""] }] }, 1, 0] },
              hasPublishedAt: { $cond: [{ $and: [{ $ne: ["$published_at", null] }, { $ne: ["$published_at", ""] }] }, 1, 0] },
              hasChannelId: { $cond: [{ $and: [{ $ne: ["$channel_id", null] }, { $ne: ["$channel_id", ""] }] }, 1, 0] },
              hasVideoDescription: { $cond: [{ $and: [{ $ne: ["$video_description", null] }, { $ne: ["$video_description", ""] }] }, 1, 0] },
              hasChannelTitle: { $cond: [{ $and: [{ $ne: ["$channel_title", null] }, { $ne: ["$channel_title", ""] }] }, 1, 0] },
              
              // Numeric fields
              hasViewCount: { $cond: [{ $and: [{ $ne: ["$view_count", null] }, { $ne: ["$view_count", ""] }] }, 1, 0] },
              hasLikeCount: { $cond: [{ $and: [{ $ne: ["$like_count", null] }, { $ne: ["$like_count", ""] }] }, 1, 0] },
              hasCommentCount: { $cond: [{ $and: [{ $ne: ["$comment_count", null] }, { $ne: ["$comment_count", ""] }] }, 1, 0] },
              
              // Array fields
              hasTopicCategories: { 
                $cond: [
                  { $and: [
                    { $ne: ["$topic_categories", null] },
                    { $ne: [{ $size: { $ifNull: ["$topic_categories", []] } }, 0] }
                  ]},
                  1, 0
                ]
              },
              hasTags: { 
                $cond: [
                  { $and: [
                    { $ne: ["$tags", null] },
                    { $ne: [{ $size: { $ifNull: ["$tags", []] } }, 0] }
                  ]},
                  1, 0
                ]
              }
            }
          },
          {
            $group: {
              _id: null,
              videoTitleCount: { $sum: "$hasVideoTitle" },
              videoTitleEmpty: { $sum: { $cond: ["$hasVideoTitle", 0, 1] } },
              
              videoIdCount: { $sum: "$hasVideoId" },
              videoIdEmpty: { $sum: { $cond: ["$hasVideoId", 0, 1] } },
              
              publishedAtCount: { $sum: "$hasPublishedAt" },
              publishedAtEmpty: { $sum: { $cond: ["$hasPublishedAt", 0, 1] } },
              
              channelIdCount: { $sum: "$hasChannelId" },
              channelIdEmpty: { $sum: { $cond: ["$hasChannelId", 0, 1] } },
              
              videoDescriptionCount: { $sum: "$hasVideoDescription" },
              videoDescriptionEmpty: { $sum: { $cond: ["$hasVideoDescription", 0, 1] } },
              
              channelTitleCount: { $sum: "$hasChannelTitle" },
              channelTitleEmpty: { $sum: { $cond: ["$hasChannelTitle", 0, 1] } },
              
              viewCountCount: { $sum: "$hasViewCount" },
              viewCountEmpty: { $sum: { $cond: ["$hasViewCount", 0, 1] } },
              
              likeCountCount: { $sum: "$hasLikeCount" },
              likeCountEmpty: { $sum: { $cond: ["$hasLikeCount", 0, 1] } },
              
              commentCountCount: { $sum: "$hasCommentCount" },
              commentCountEmpty: { $sum: { $cond: ["$hasCommentCount", 0, 1] } },
              
              topicCategoriesCount: { $sum: "$hasTopicCategories" },
              topicCategoriesEmpty: { $sum: { $cond: ["$hasTopicCategories", 0, 1] } },
              
              tagsCount: { $sum: "$hasTags" },
              tagsEmpty: { $sum: { $cond: ["$hasTags", 0, 1] } }
            }
          }
        ]
      }
    },
    {
      $project: {
        totalDocuments: { $arrayElemAt: ["$totalDocuments.count", 0] },
        fieldAnalysis: { $arrayElemAt: ["$fieldAnalysis", 0] }
      }
    }
      ])
  .forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/channel_video_empty_fields.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);