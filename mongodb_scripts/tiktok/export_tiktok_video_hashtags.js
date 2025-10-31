// Export per-video hashtag rows and a unique hashtag table
(function () {
  try {
    const outPathFlat = "/opt/airflow/mongodb_scripts/output/tiktok/tiktok_video_hashtags_flat.json";
    const outPathUnique = "/opt/airflow/mongodb_scripts/output/tiktok/tiktok_video_hashtags_unique.json";

    // Flat rows
    const flatCursor = db.tiktok_user_video.aggregate([
      { $project: { _id: 0, username: 1, video_id: 1, hashtag_names: 1 } },
      { $unwind: { path: "$hashtag_names", preserveNullAndEmptyArrays: true } },
      { $project: { username: 1, video_id: 1, hashtag_name: "$hashtag_names" } }
    ]);
    const flat = []; flatCursor.forEach(doc => flat.push(doc));
    fs.writeFileSync(outPathFlat, JSON.stringify(flat, null, 2));

    // Unique with counts
    const uniqueCursor = db.tiktok_user_video.aggregate([
      { $project: { _id: 0, video_id: 1, username: 1, hashtag_names: 1 } },
      { $unwind: { path: "$hashtag_names", preserveNullAndEmptyArrays: false } },
      { $group: { _id: "$hashtag_names", occurrences: { $sum: 1 }, videos: { $addToSet: "$video_id" }, users: { $addToSet: "$username" } } },
      { $project: { _id: 0, hashtag_name: "$_id", occurrences: 1, distinct_videos: { $size: "$videos" }, distinct_users: { $size: "$users" } } },
      { $sort: { occurrences: -1, hashtag_name: 1 } }
    ]);
    const uniq = []; uniqueCursor.forEach(doc => uniq.push(doc));
    fs.writeFileSync(outPathUnique, JSON.stringify(uniq, null, 2));

    print("Exported", flat.length, "hashtag rows and", uniq.length, "unique hashtags.");
  } catch (e) {
    print("❌ Error exporting TikTok hashtags:", e.message || e);
    throw e;
  }
})();
