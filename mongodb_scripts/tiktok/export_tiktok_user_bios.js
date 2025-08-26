
const cursor = db.tiktok_user_info.find(
  { bio_description: { $exists: true, $ne: "" } },
  { username: 1, bio_description: 1, _id: 0 }
);

const userBios = [];

cursor.forEach(doc => {
  if (doc.username && doc.bio_description) {
    userBios.push({
      username: doc.username.toLowerCase(),
      bio_description: doc.bio_description
    });
  }
});


const outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/user_bios.json";
fs.writeFileSync(outputPath, JSON.stringify(userBios, null, 2));
print("Exported " + userBios.length + " users to " + outputPath);
