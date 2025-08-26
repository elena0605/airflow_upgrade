// Store the results in a variable
var results = [];
db.tiktok_user_info.find(
    {},
    { username: 1, likes_count: 1, _id: 0 }
)
.sort({ likes_count: -1 })
.forEach(function(doc) {
    // Convert the likes_count to a regular number
    results.push({
        username: doc.username,
        likes_count: NumberLong(doc.likes_count).toNumber()  // Convert Long to regular number
    });
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/likes_count.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);