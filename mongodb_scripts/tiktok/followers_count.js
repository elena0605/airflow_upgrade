var results = [];
db.tiktok_user_info.find(
    {},
    { username: 1, follower_count: 1, _id: 0 }
  )
  .sort({ follower_count: -1 }).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/followers_count.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);
