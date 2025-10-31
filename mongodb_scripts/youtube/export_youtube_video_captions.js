// Export per-caption flattened records for completeness analyses
// Requires global `db` bound to the target database (handled by mongodb_analysis.js)

(function () {
  try {
    const outPath = "/opt/airflow/mongodb_scripts/output/youtube/youtube_video_captions_flat.json";
    const cursor = db.youtube_video_captions.aggregate([
      {
        $project: {
          _id: 0,
          caption_id: 1,
          video_id: 1,
          language: 1,
          language_name: 1,
          track_kind: 1,
          is_auto: 1,
          is_draft: 1,
          caption_content: 1,
          fetched_time: 1
        }
      }
    ]);

    const results = [];
    cursor.forEach(doc => results.push(doc));

    fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
    print("Exported", results.length, "captions to", outPath);
  } catch (e) {
    print("❌ Error exporting YouTube video captions:", e.message || e);
    throw e;
  }
})();
