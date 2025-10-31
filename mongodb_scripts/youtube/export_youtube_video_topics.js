// Export per-video topic rows for completeness analyses
// Each row represents a (video_id, topic_category) association

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/youtube/youtube_video_topics_flat.json";
    const cursor = db.youtube_channel_videos.aggregate([
      { $project: { _id: 0, video_id: 1, channel_id: 1, channel_title: 1, topic_categories: 1 } },
      { $unwind: { path: "$topic_categories", preserveNullAndEmptyArrays: true } },
      { $project: { video_id: 1, channel_id: 1, channel_title: 1, topic_category: "$topic_categories" } }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));

    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "video-topic rows to", outPath);
  } catch (e) {
    print("❌ Error exporting YouTube video topics:", e.message || e);
    throw e;
  }
})();
