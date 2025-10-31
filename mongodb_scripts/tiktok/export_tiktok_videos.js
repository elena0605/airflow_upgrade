// Export per-video flattened TikTok records
// Source collection: tiktok_user_video
// Output JSON (auto-CSV via mongodb_analysis.js): output/tiktok/tiktok_videos_flat.json

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/tiktok/tiktok_videos_flat.json";
    const cursor = db.tiktok_user_video.aggregate([
      {
        $project: {
          _id: 0,
          username: 1,
          video_id: 1,
          create_time: 1,
          region_code: 1,
          share_count: 1,
          view_count: 1,
          like_count: 1,
          comment_count: 1,
          music_id: 1,
          voice_to_text: 1,
          is_stem_verified: 1,
          video_duration: 1,
          video_title: 1,
          video_author_url: 1,
          video_thumbnail_url: 1,
          video_mention_list: 1,
          video_label_content: "$video_label.content",
          video_tag_type: 1,
          search_id: 1,
          video_description: 1,
          hashtag_names: 1,
          hashtag_info_list: 1,
          sticker_list: 1
        }
      }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));
    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "TikTok videos to", outPath);
  } catch (e) {
    print("❌ Error exporting TikTok videos:", e.message || e);
    throw e;
  }
})();
