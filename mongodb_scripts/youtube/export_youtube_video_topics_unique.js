// Export unique topic values with frequency and distinct counts
(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/youtube/youtube_video_topics_unique.json";
    const cursor = db.youtube_channel_videos.aggregate([
      { $project: { _id: 0, video_id: 1, channel_id: 1, topic_categories: 1 } },
      { $unwind: { path: "$topic_categories", preserveNullAndEmptyArrays: false } },
      { $group: {
          _id: "$topic_categories",
          occurrences: { $sum: 1 },
          videos: { $addToSet: "$video_id" },
          channels: { $addToSet: "$channel_id" }
      }},
      { $project: {
          _id: 0,
          topic_category: "$_id",
          occurrences: 1,
          distinct_videos: { $size: "$videos" },
          distinct_channels: { $size: "$channels" }
      }},
      { $sort: { occurrences: -1, topic_category: 1 } }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "unique topics to", outPath);
  } catch (e) {
    print("❌ Error exporting unique topics:", e.message || e);
    throw e;
  }
})();
