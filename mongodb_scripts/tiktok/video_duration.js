var results = [];
db.tiktok_user_video.aggregate([
  {
    $match: {
      video_duration: { $exists: true, $type: "number", $gte: 0 }
    }
  },
  {
    $facet: {
      "totalCount": [{ $count: "count" }],
      "durationGroups": [
        {
          $project: {
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
            _id: "$duration_category",
            count: { $sum: 1 }
          }
        }
      ]
    }
  },
  {
    $project: {
      statistics: {
        $map: {
          input: "$durationGroups",
          as: "group",
          in: {
            category: "$$group._id",
            count: "$$group.count",
            percentage: {
              $cond: {
                if: { $gt: [{ $arrayElemAt: ["$totalCount.count", 0] }, 0] },
                then: {
                  $round: [
                    {
                      $multiply: [
                        { $divide: ["$$group.count", { $arrayElemAt: ["$totalCount.count", 0] }] },
                        100
                      ]
                    },
                    2
                  ]
                },
                else: 0
              }
            }
          }
        }
      }
    }
  },
  { $unwind: "$statistics" },
  { $sort: { "statistics.category": 1 } },
  {
    $project: {
      category: "$statistics.category",
      count: "$statistics.count",
      percentage: "$statistics.percentage",
      _id: 0
    }
  }
]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/video_duration.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));