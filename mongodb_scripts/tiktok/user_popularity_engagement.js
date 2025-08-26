var results = [];
db.tiktok_user_info.aggregate([
    {
      $project: {
        username: 1,
        follower_count: { $ifNull: [{ $toLong: "$follower_count" }, 0] }, 
        likes_count: { $ifNull: [{ $toLong: "$likes_count" }, 0] },
        video_count: { $ifNull: [{ $toLong: "$video_count" }, 0] },
  
        // Engagement Rate = (average likes per video) / follower_count * 100
        engagement_rate: {
          $cond: {
            if: { $and: [{ $gt: ["$video_count", 0] }, { $gt: ["$follower_count", 0] }] },
            then: {
              $round: [
                {
                  $multiply: [
                    {
                      $divide: [
                        { $divide: [{ $toLong: "$likes_count" }, { $toLong: "$video_count" }] }, // avg likes per video
                        { $toLong: "$follower_count" }
                      ]
                    },
                    100 // Convert to percentage
                  ]
                },
                2
              ]
            },
            else: 0
          }
        },
  
        // Popularity Score = weighted combination of normalized metrics
        popularity_score: {
          $round: [
            {
              $multiply: [
                {
                  $add: [
                    { $multiply: [{ $divide: [{ $toLong: "$follower_count" }, 1000000] }, 0.6] }, // 60% weight to followers (in millions)
                    { $multiply: [{ $divide: [{ $toLong: "$likes_count" }, 1000000] }, 0.4] }     // 40% weight to likes (in millions)
                  ]
                },
                100 // Scale to 0-100 range
              ]
            },
            2
          ]
        },
        _id: 0
      }
    },
    {
      $sort: { popularity_score: -1 }
    }
]).forEach(function(doc) {
    // Convert any remaining Long numbers to regular numbers
    results.push({
        username: doc.username,
        follower_count: NumberLong(doc.follower_count).toNumber(),
        likes_count: NumberLong(doc.likes_count).toNumber(),
        video_count: NumberLong(doc.video_count).toNumber(),
        engagement_rate: doc.engagement_rate,
        popularity_score: doc.popularity_score
    });
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/user_popularity_engagement.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);
