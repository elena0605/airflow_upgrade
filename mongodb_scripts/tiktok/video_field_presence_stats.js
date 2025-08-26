var results = [];
db.tiktok_user_video.aggregate([
    {
      $facet: {
        "totalVideos": [
          { $count: "count" }
        ],
        "videosWithVoice": [
          {
            $match: {
              voice_to_text: { 
                $exists: true, 
                $ne: null,
                $not: { $in: ["", " ", "undefined", "No voice found.", "No speech found."] },
                $regex: /\S/  // Contains at least one non-whitespace character
              }
            }
          },
          { $count: "count" }
        ],
        "videosWithDescription": [
          {
            $match: {
              video_description: { 
                $exists: true,
                $ne: null,
                $regex: /\S/  // Must contain at least one non-whitespace character
              }
            }
          },
          { $count: "count" }
        ],
        "videosWithHashtags": [
          {
            $match: {
              $and: [
                { hashtag_names: { $exists: true, $ne: null } },
                { $expr: { $isArray: "$hashtag_names" } },
                { $expr: { $gt: [{ $size: "$hashtag_names" }, 0] } }
              ]
            }
          },
          { $count: "count" }
        ],
        "videosWithHashtagInfo": [
          {
            $match: {
              $and: [
                { hashtag_info_list: { $exists: true, $ne: null } },
                { $expr: { $isArray: "$hashtag_info_list" } },
                { $expr: { $gt: [{ $size: "$hashtag_info_list" }, 0] } }
              ]
            }
          },
          { $count: "count" }
        ],
        "videosWithMentions": [
          {
            $match: {
              $and: [
                { video_mention_list: { $exists: true, $ne: null } },
                { $expr: { $isArray: "$video_mention_list" } },
                { $expr: { $gt: [{ $size: "$video_mention_list" }, 0] } }
              ]
            }
          },
          { $count: "count" }
        ]
      }
    },
    {
      $project: {
        total_videos: { $arrayElemAt: ["$totalVideos.count", 0] },
        videos_with_voice: { $ifNull: [{ $arrayElemAt: ["$videosWithVoice.count", 0] }, 0] },
        videos_with_description: { $ifNull: [{ $arrayElemAt: ["$videosWithDescription.count", 0] }, 0] },
        videos_with_hashtags: { $ifNull: [{ $arrayElemAt: ["$videosWithHashtags.count", 0] }, 0] },
        videos_with_hashtag_info: { $ifNull: [{ $arrayElemAt: ["$videosWithHashtagInfo.count", 0] }, 0] },
        videos_with_mentions: { $ifNull: [{ $arrayElemAt: ["$videosWithMentions.count", 0] }, 0] }
      }
    },
    {
      $addFields: {
        voice_percentage: { 
          $multiply: [
            { $divide: ["$videos_with_voice", { $max: [1, "$total_videos"] }] }, 
            100
          ] 
        },
        description_percentage: { 
          $multiply: [
            { $divide: ["$videos_with_description", { $max: [1, "$total_videos"] }] }, 
            100
          ] 
        },
        hashtags_percentage: { 
          $multiply: [
            { $divide: ["$videos_with_hashtags", { $max: [1, "$total_videos"] }] }, 
            100
          ] 
        },
        hashtag_info_percentage: { 
          $multiply: [
            { $divide: ["$videos_with_hashtag_info", { $max: [1, "$total_videos"] }] }, 
            100
          ] 
        },
        mentions_percentage: { 
          $multiply: [
            { $divide: ["$videos_with_mentions", { $max: [1, "$total_videos"] }] }, 
            100
          ] 
        }
      }
    }
  ]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/video_field_presence_stats.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath); 