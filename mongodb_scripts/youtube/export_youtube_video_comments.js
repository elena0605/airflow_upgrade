// Export per-comment flattened records for completeness analyses
// Requires global `db` bound to the target database (handled by mongodb_analysis.js)

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/youtube/youtube_video_comments_flat.json";
    const cursor = db.youtube_video_comments.aggregate([
      {
        $project: {
          _id: 0,
          comment_id: 1,
          video_id: 1,
          channel_id: 1,
          text: 1,
          authorDisplayName: 1,
          authorProfileImageUrl: 1,
          authorChannelUrl: 1,
          canRate: 1,
          viewerRating: 1,
          likeCount: { $toDouble: "$likeCount" },
          canReply: 1,
          totalReplyCount: { $toDouble: "$totalReplyCount" },
          publishedAt: 1,
          updatedAt: 1
        }
      }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));

    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "comments to", outPath);
  } catch (e) {
    print("❌ Error exporting YouTube video comments:", e.message || e);
    throw e;
  }
})();
