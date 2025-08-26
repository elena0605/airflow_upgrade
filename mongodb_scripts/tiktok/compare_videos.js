var results = [];
db.tiktok_user_info.aggregate([
    {
        $lookup: {
            from: "tiktok_user_video",
            localField: "username",
            foreignField: "username",
            as: "videos"
        }
    },
    {
        $project: {
            username: 1,
            declared_video_count: "$video_count",
            actual_video_count: { $size: "$videos" },
            _id: 0
        }
    },
    {
        $sort: { actual_video_count: -1 }
    }
]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/compare_videos.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);
