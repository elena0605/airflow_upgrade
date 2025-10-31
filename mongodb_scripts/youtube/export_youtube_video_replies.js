// Export per-reply flattened records for completeness analyses
// Requires global `db` bound to the target database (handled by mongodb_analysis.js)

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/youtube/youtube_video_replies_flat.json";
    const cursor = db.youtube_video_replies.aggregate([
      {
        $project: {
          _id: 0,
          reply_id: 1,
          parent_id: 1,
          channel_id: 1,
          text: 1,
          authorDisplayName: 1,
          authorProfileImageUrl: 1,
          authorChannelUrl: 1,
          canRate: 1,
          viewerRating: 1,
          likeCount: { $toDouble: "$likeCount" },
          publishedAt: 1,
          updatedAt: 1
        }
      }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));

    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "replies to", outPath);
  } catch (e) {
    print("❌ Error exporting YouTube video replies:", e.message || e);
    throw e;
  }
})();
