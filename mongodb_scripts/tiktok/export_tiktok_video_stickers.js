// Export per-video sticker rows and a unique sticker table
(function () {
  try {
    const outPathFlat = "/opt/airflow/mongodb_scripts/output/tiktok/tiktok_video_stickers_flat.json";
    const outPathUnique = "/opt/airflow/mongodb_scripts/output/tiktok/tiktok_video_stickers_unique.json";

    // Flat rows
    const flatCursor = db.tiktok_user_video.aggregate([
      { $project: { _id: 0, username: 1, video_id: 1, sticker_list: 1 } },
      { $unwind: { path: "$sticker_list", preserveNullAndEmptyArrays: true } },
      { $project: { username: 1, video_id: 1, sticker_id: "$sticker_list.sticker_id", sticker_name: "$sticker_list.sticker_name" } }
    ]);
    const flat = []; flatCursor.forEach(doc => flat.push(doc));
    fs.writeFileSync(outPathFlat, JSON.stringify(flat, null, 2));

    // Unique with counts
    const uniqueCursor = db.tiktok_user_video.aggregate([
      { $project: { _id: 0, video_id: 1, username: 1, sticker_list: 1 } },
      { $unwind: { path: "$sticker_list", preserveNullAndEmptyArrays: false } },
      { $group: { _id: { id: "$sticker_list.sticker_id", name: "$sticker_list.sticker_name" }, occurrences: { $sum: 1 }, videos: { $addToSet: "$video_id" }, users: { $addToSet: "$username" } } },
      { $project: { _id: 0, sticker_id: "$_id.id", sticker_name: "$_id.name", occurrences: 1, distinct_videos: { $size: "$videos" }, distinct_users: { $size: "$users" } } },
      { $sort: { occurrences: -1, sticker_name: 1 } }
    ]);
    const uniq = []; uniqueCursor.forEach(doc => uniq.push(doc));
    fs.writeFileSync(outPathUnique, JSON.stringify(uniq, null, 2));

    print("Exported", flat.length, "sticker rows and", uniq.length, "unique stickers.");
  } catch (e) {
    print("❌ Error exporting TikTok stickers:", e.message || e);
    throw e;
  }
})();
