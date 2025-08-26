const fs = require("fs");

// Query documents that have non-empty description and username
const cursor = db.youtube_channel_stats.find(
  { description: { $exists: true, $ne: "" }, username: { $exists: true, $ne: "" } },
  { username: 1, description: 1, _id: 0 }
);

const userDescriptions = [];

cursor.forEach(doc => {
  userDescriptions.push({
    username: doc.username.toLowerCase().trim(),
    description: doc.description
  });
});

const outputPath = "/opt/airflow/mongodb_scripts/output/youtube/user_descriptions.json";
fs.writeFileSync(outputPath, JSON.stringify(userDescriptions, null, 2));
print("Exported " + userDescriptions.length + " users to " + outputPath);
