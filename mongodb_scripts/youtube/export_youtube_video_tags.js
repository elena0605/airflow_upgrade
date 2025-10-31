// Export per-video tag rows for completeness analyses
// Each row represents a (video_id, tag_name) association

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/youtube/youtube_video_tags_flat.json";
    const cursor = db.youtube_channel_videos.aggregate([
      { $project: { _id: 0, video_id: 1, channel_id: 1, channel_title: 1, tags: 1 } },
      { $unwind: { path: "$tags", preserveNullAndEmptyArrays: true } },
      { $project: { video_id: 1, channel_id: 1, channel_title: 1, tag_name: "$tags" } }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));

    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "video-tag rows to", outPath);
  } catch (e) {
    print("❌ Error exporting YouTube video tags:", e.message || e);
    throw e;
  }
})();
