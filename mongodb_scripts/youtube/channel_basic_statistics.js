// Store the results in a variable
var results = [];
db.youtube_channel_stats.aggregate([
    {
      $project: {
        _id: 0,
        "Channel": "$title",
        "Views": { $toDouble: "$view_count" },
        "Subscribers": { $toDouble: "$subscriber_count" },
        "Videos": { $toDouble: "$video_count" },
        "Views Per Video": { 
          $divide: [
            { $toDouble: "$view_count" },
            { $cond: [
              { $eq: [{ $toDouble: "$video_count" }, 0] },
              1,
              { $toDouble: "$video_count" }
            ]}
          ]
        },
        "Views Per Subscriber": { 
          $divide: [
            { $toDouble: "$view_count" },
            { $cond: [
              { $eq: [{ $toDouble: "$subscriber_count" }, 0] },
              1,
              { $toDouble: "$subscriber_count" }
            ]}
          ]
        }
      }
    },
    {
      $sort: { "Subscribers": -1 }
    }
]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/channel_basic_statistics.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);