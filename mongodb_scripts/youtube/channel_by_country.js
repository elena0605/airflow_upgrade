var results = [];
// First, get channels with unknown countries
db.youtube_channel_stats.aggregate([
    {
      $match: {
        $or: [
          { country: null },
          { country: "" },
          { country: { $exists: false } }
        ]
      }
    },
    {
      $project: {
        _id: 0,
        "Channel": "$title",
        "Country": "$country",
        "Views": { $toDouble: "$view_count" },
        "Subscribers": { $toDouble: "$subscriber_count" }
      }
    },
    {
      $sort: { "Subscribers": -1 }
    }
  ])
  
  // Then, get country statistics including unknown count
  db.youtube_channel_stats.aggregate([
    {
      $project: {
        country: { 
          $cond: [
            { $or: [
              { $eq: ["$country", null] },
              { $eq: ["$country", ""] },
              { $eq: [{ $type: "$country" }, "missing"] }
            ]},
            "Unknown",
            "$country"
          ]
        },
        title: 1,
        view_count: 1,
        subscriber_count: 1
      }
    },
    {
      $group: {
        _id: "$country",
        channelCount: { $sum: 1 },
        totalViews: { $sum: { $toDouble: "$view_count" } },
        totalSubs: { $sum: { $toDouble: "$subscriber_count" } },
        channels: { $push: "$title" }
      }
    },
    { 
      $sort: { channelCount: -1 } 
    },
    {
      $project: {
        _id: 0,
        "Country": "$_id",
        "Channel Count": "$channelCount",
        "Total Views": "$totalViews",
        "Total Subscribers": "$totalSubs",
        "Channels": "$channels"
      }
    }
    ]).forEach(function(doc) {
        results.push(doc);
    });

    // Save to file
    var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/channel_by_country.json";
    fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
    print("Results saved to: " + outputPath);
