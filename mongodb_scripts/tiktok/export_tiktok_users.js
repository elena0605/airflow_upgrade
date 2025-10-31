// Export per-user flattened TikTok records
// Source collection: tiktok_user_info
// Output JSON (auto-CSV via mongodb_analysis.js): output/tiktok/tiktok_users_flat.json

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/tiktok/tiktok_users_flat.json";
    const cursor = db.tiktok_user_info.aggregate([
      {
        $project: {
          _id: 0,
          username: 1,
          display_name: 1,
          bio_description: 1,
          bio_url: 1,
          avatar_url: 1,
          follower_count: 1,
          following_count: 1,
          likes_count: 1,
          video_count: 1,
          is_verified: 1
        }
      }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "TikTok users to", outPath);
  } catch (e) {
    print("❌ Error exporting TikTok users:", e.message || e);
    throw e;
  }
})();
