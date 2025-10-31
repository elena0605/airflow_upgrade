// Export per-comment flattened TikTok records directly to CSV to avoid huge JSON strings
// Source collection: tiktok_video_comments

(function () {
  try {
    const outCsv = "/opt/airflow/mongodb_scripts/output/tiktok/tiktok_video_comments_flat.csv";
    const headers = [
      "comment_id","text","like_count","reply_count","create_time","username","video_id"
    ];

    function csvEscape(value) {
      if (value === null || value === undefined) return "";
      const s = String(value);
      return (s.includes(',') || s.includes('"') || s.includes('\n')) ? '"' + s.replace(/"/g, '""') + '"' : s;
    }

    // Write header
    fs.writeFileSync(outCsv, headers.join(',') + '\n');

    const cursor = db.tiktok_video_comments.aggregate([
      { $project: {
          _id: 0,
          comment_id: 1,
          text: { $substrBytes: [{$ifNull: ["$text", ""]}, 0, 8000] },
          like_count: { $toLong: "$like_count" },
          reply_count: { $toLong: "$reply_count" },
          create_time: 1,
          username: 1,
          video_id: 1
      }}
    ], { allowDiskUse: true });

    let written = 0;
    cursor.forEach(doc => {
      const row = [
        csvEscape(doc.comment_id),
        csvEscape(doc.text),
        csvEscape(doc.like_count),
        csvEscape(doc.reply_count),
        csvEscape(doc.create_time),
        csvEscape(doc.username),
        csvEscape(doc.video_id)
      ].join(',') + '\n';
      fs.appendFileSync(outCsv, row);
      written++;
    });

    print("Exported", written, "TikTok comments to", outCsv);
  } catch (e) {
    print("❌ Error exporting TikTok comments:", e.message || e);
    throw e;
  }
})();
