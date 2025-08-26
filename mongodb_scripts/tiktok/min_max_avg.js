var results = [];
db.tiktok_user_video.aggregate([
    {
      $match: {
        share_count: { $exists: true, $type: "number", $gte: 0 },  // Filter out invalid share_count
        view_count: { $exists: true, $type: "number", $gte: 0 },    // Filter out invalid view_count
        like_count: { $exists: true, $type: "number", $gte: 0 },    // Filter out invalid like_count
        comment_count: { $exists: true, $type: "number", $gte: 0 }  // Filter out invalid comment_count
      }
    },
    {
      $group: {
        _id: "$username",
        totalVideos: { $sum: 1 },
        minShareCount: { $min: "$share_count" },
        avgShareCount: { $avg: "$share_count" },
        maxShareCount: { $max: "$share_count" },
        minViewCount: { $min: "$view_count" },
        avgViewCount: { $avg: "$view_count" },
        maxViewCount: { $max: "$view_count" },
        minLikeCount: { $min: "$like_count" },
        avgLikeCount: { $avg: "$like_count" },
        maxLikeCount: { $max: "$like_count" },
        minCommentCount: { $min: "$comment_count" },
        avgCommentCount: { $avg: "$comment_count" },
        maxCommentCount: { $max: "$comment_count" }
      }
    },
    {
      $sort: { totalVideos: -1 }  // Sort by totalVideos in descending order
    },
    {
      $project: {
        _id: 0,
        username: "$_id",
        avgCommentCount: { $round: ["$avgCommentCount", 2] },
        avgLikeCount: { $round: ["$avgLikeCount", 2] },
        avgShareCount: { $round: ["$avgShareCount", 2] },
        avgViewCount: { $round: ["$avgViewCount", 2] },
        maxCommentCount: 1,
        maxLikeCount: 1,
        maxShareCount: 1,
        maxViewCount: 1,
        minCommentCount: 1,
        minLikeCount: 1,
        minShareCount: 1,
        minViewCount: 1,
        totalVideos: 1
      }
    }
  ]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/min_max_avg.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));