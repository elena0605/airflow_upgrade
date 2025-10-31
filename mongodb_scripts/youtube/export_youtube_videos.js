// Export per-video flattened records for completeness analyses
// Requires global `db` bound to the target database (handled by mongodb_analysis.js)

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/youtube/youtube_videos_flat.json";
    const cursor = db.youtube_channel_videos.aggregate([
      {
        $project: {
          _id: 0,
          video_id: 1,
          video_title: 1,
          published_at: 1,
          channel_id: 1,
          channel_title: 1,
          video_description: 1,
          view_count: { $toDouble: "$view_count" },
          like_count: { $toDouble: "$like_count" },
          comment_count: { $toDouble: "$comment_count" },
          topic_categories: 1,
          tags: 1,
          thumbnail_gridfs_id: "$thumbnails.gridfs_id"
        }
      }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));

    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "videos to", outPath);
  } catch (e) {
    print("❌ Error exporting YouTube videos:", e.message || e);
    throw e;
  }
})();
