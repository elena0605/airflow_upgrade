var results = [];
db.tiktok_user_video.aggregate([
    // Group by username
    {
      $match: {
        video_duration: { $exists: true, $type: "number", $gte: 0 }
      }
    },
    {
      $project: {
        username: 1,
        duration_category: {
          $switch: {
            branches: [
              { case: { $lt: ["$video_duration", 15] }, then: "SHORT (<15s)" },
              { 
                case: { 
                  $and: [
                    { $gte: ["$video_duration", 15] },
                    { $lt: ["$video_duration", 60] }
                  ]
                },
                then: "MID (15-60s)" 
              },
              {
                case: {
                  $and: [
                    { $gte: ["$video_duration", 60] },
                    { $lt: ["$video_duration", 300] }
                  ]
                },
                then: "LONG (1-5min)"
              }
            ],
            default: "EXTRA_LONG (>5min)"
          }
        }
      }
    },
    {
      $group: {
        _id: {
          username: "$username",
          category: "$duration_category"
        },
        count: { $sum: 1 }
      }
    },
    {
      $group: {
        _id: "$_id.username",
        total_videos: { $sum: "$count" },
        duration_categories: { 
          $push: { 
            category: "$_id.category", 
            count: "$count"
          } 
        }
      }
    },
    {
      $addFields: {
        duration_categories: {
          $map: {
            input: "$duration_categories",
            as: "category",
            in: {
              category: "$$category.category",
              count: "$$category.count",
              percentage: {
                $round: [
                  {
                    $multiply: [
                      { $divide: ["$$category.count", "$total_videos"] },
                      100
                    ]
                  },
                  2
                ]
              }
            }
          }
        }
      }
    },
    {
      $project: {
        _id: 0,
        username: "$_id",
        total_videos: 1,
        duration_categories: 1
      }
    },
    {
      $sort: { "total_videos": -1 }
    }
  ]).forEach(function(doc) {
    results.push(doc);
  });

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/user_video_duration.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));