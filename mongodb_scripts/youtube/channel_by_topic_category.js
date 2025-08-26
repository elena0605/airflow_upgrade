var results = [];
db.youtube_channel_stats.aggregate([
    // Stage 1: Unwind the topic_categories array
    { $unwind: "$topic_categories" },
    
    // Stage 2: Group by topic category
    {
      $group: {
        _id: "$topic_categories",
        channelCount: { $sum: 1 },
        avgViews: { $avg: { $toDouble: "$view_count" } },
        avgSubs: { $avg: { $toDouble: "$subscriber_count" } },
        totalVideos: { $sum: { $toDouble: "$video_count" } },
        channels: { 
          $push: { 
            title: "$title", 
            subscribers: { $toDouble: "$subscriber_count" },
            videos: { $toDouble: "$video_count" }
          } 
        }
      }
    },
    
    // Stage 3: Add calculated fields
    {
      $project: {
        _id: 0,
        category: "$_id",
        channelCount: 1,
        avgViews: 1,
        avgSubs: 1,
        totalVideos: 1,
        channels: 1
      }
    },
    
    // Stage 4: Sort by most popular categories
    { $sort: { channelCount: -1 } }
    ]).forEach(function(doc) {
    results.push(doc);
});
  
// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/channel_by_topic_category.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);