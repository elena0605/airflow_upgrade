// Store the results in a variable
var results = [];
db.youtube_channel_stats.aggregate([
    {
      $lookup: {
        from: "youtube_channel_videos",
        localField: "channel_id",
        foreignField: "channel_id",
        as: "collected_videos"
      }
    },
    {
      $project: {
        _id: 0,
        "Channel": "$title",
        "channel_id": "$channel_id",
        "Views": { $toDouble: "$view_count" },
        "Subscribers": { $toDouble: "$subscriber_count" },
        "TotalVideos": { $toDouble: "$video_count" },
        "CollectedVideos": { $size: "$collected_videos" },
        "Views Per Video": { 
          $divide: [
            { $toDouble: "$view_count" },
            { $cond: [
              { $eq: [{ $size: "$collected_videos" }, 0] },
              1,
              { $size: "$collected_videos" }
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