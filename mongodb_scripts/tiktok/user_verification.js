var results = [];
db.tiktok_user_info.aggregate([
    {
        $facet: {
            "verified": [
                { $match: { is_verified: true } },
                { 
                    $project: {
                        username: 1,
                        _id: 0
                    }
                },
                { $sort: { username: 1 } }
            ],
            "not_verified": [
                { $match: { is_verified: false } },
                { 
                    $project: {
                        username: 1,
                        _id: 0
                    }
                },
                { $sort: { username: 1 } }
            ],
            "total": [
                { $count: "count" }
            ]
        }
    },
    {
        $project: {
            "Verified Users": "$verified.username",
            "Non-Verified Users": "$not_verified.username",
            "Verified Count": { $size: "$verified" },
            "Non-Verified Count": { $size: "$not_verified" },
            "Verified Percentage": {
                $multiply: [
                    { 
                        $divide: [
                            { $size: "$verified" },
                            { $arrayElemAt: ["$total.count", 0] }
                        ]
                    },
                    100
                ]
            },
            "Non-Verified Percentage": {
                $multiply: [
                    { 
                        $divide: [
                            { $size: "$not_verified" },
                            { $arrayElemAt: ["$total.count", 0] }
                        ]
                    },
                    100
                ]
            }
        }
    }
    ]).forEach(function(doc) {
        results.push(doc);
    });

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/user_verification.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);