var results = [];
db.tiktok_user_video.aggregate([
    // Group by username
    {
        $group: {
            _id: "$username",
            total_videos: { $sum: 1 },
            
            // Count videos with actual content in each field
            videos_with_hashtag_names: { 
                $sum: { 
                    $cond: [
                        { $and: [
                            { $isArray: "$hashtag_names" },
                            { $gt: [{ $size: { $ifNull: ["$hashtag_names", []] } }, 0] }
                        ]},
                        1, 0
                    ]
                }
            },
            videos_with_hashtag_info: { 
                $sum: { 
                    $cond: [
                        { $and: [
                            { $isArray: "$hashtag_info_list" },
                            { $gt: [{ $size: { $ifNull: ["$hashtag_info_list", []] } }, 0] }
                        ]},
                        1, 0
                    ]
                }
            },
            videos_with_description: { 
                $sum: { 
                    $cond: [
                        { $and: [
                            { $ne: ["$video_description", null] },
                            { $ne: ["$video_description", ""] },
                            { $ne: [{ $type: "$video_description" }, "missing"] }
                        ]},
                        1, 0
                    ]
                }
            },
            videos_with_voice_to_text: { 
                $sum: { 
                    $cond: [
                        { $and: [
                            { $ne: ["$voice_to_text", null] },
                            { $ne: ["$voice_to_text", ""] },
                            { $ne: [{ $type: "$voice_to_text" }, "missing"] }
                        ]},
                        1, 0
                    ]
                }
            },
            videos_with_video_label_content: { 
                $sum: { 
                    $cond: [
                        { $and: [
                            { $ne: ["$video_label.content", null] },
                            { $ne: ["$video_label.content", ""] },
                            { $ne: [{ $type: "$video_label.content" }, "missing"] }
                        ]},
                        1, 0
                    ]
                }
            },
            
            // Count videos with content in all fields
            videos_with_all_fields: {
                $sum: {
                    $cond: [
                        { $and: [
                            // Hashtag names has content
                            { $isArray: "$hashtag_names" },
                            { $gt: [{ $size: { $ifNull: ["$hashtag_names", []] } }, 0] },
                            
                            // Hashtag info has content
                            { $isArray: "$hashtag_info_list" },
                            { $gt: [{ $size: { $ifNull: ["$hashtag_info_list", []] } }, 0] },
                            
                            // Description has content
                            { $ne: ["$video_description", null] },
                            { $ne: ["$video_description", ""] },
                            { $ne: [{ $type: "$video_description" }, "missing"] },
                            
                            // Voice to text has content
                            { $ne: ["$voice_to_text", null] },
                            { $ne: ["$voice_to_text", ""] },
                            { $ne: [{ $type: "$voice_to_text" }, "missing"] },
                            
                            // Video label content has content
                            { $ne: ["$video_label.content", null] },
                            { $ne: ["$video_label.content", ""] },
                            { $ne: [{ $type: "$video_label.content" }, "missing"] }
                        ]},
                        1, 0
                    ]
                }
            }
        }
    },
    
    // Calculate percentages and format output
    {
        $project: {
            username: "$_id",
            total_videos: 1,
            videos_with_hashtag_names: 1,
            videos_with_hashtag_info: 1,
            videos_with_description: 1,
            videos_with_voice_to_text: 1,
            videos_with_video_label_content: 1,
            videos_with_all_fields: 1,
            
            // Percentages
            pct_with_hashtag_names: { 
                $multiply: [{ $divide: ["$videos_with_hashtag_names", { $max: [1, "$total_videos"] }] }, 100] 
            },
            pct_with_hashtag_info: { 
                $multiply: [{ $divide: ["$videos_with_hashtag_info", { $max: [1, "$total_videos"] }] }, 100] 
            },
            pct_with_description: { 
                $multiply: [{ $divide: ["$videos_with_description", { $max: [1, "$total_videos"] }] }, 100] 
            },
            pct_with_voice_to_text: { 
                $multiply: [{ $divide: ["$videos_with_voice_to_text", { $max: [1, "$total_videos"] }] }, 100] 
            },
            pct_with_video_label_content: { 
                $multiply: [{ $divide: ["$videos_with_video_label_content", { $max: [1, "$total_videos"] }] }, 100] 
            },
            pct_with_all_fields: { 
                $multiply: [{ $divide: ["$videos_with_all_fields", { $max: [1, "$total_videos"] }] }, 100] 
            }
        }
    },
    
    // Sort by percentage of videos with all fields
    { $sort: { pct_with_all_fields: -1, total_videos: -1 } }
]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/user_videos_additional_analysis.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);
