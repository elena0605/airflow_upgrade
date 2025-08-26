var results = [];
db.tiktok_user_video.aggregate([
    // First facet to run two parallel processes
    {
        $facet: {
            // Original metrics pipeline
            "engagement_metrics": [
                {
                    $addFields: {
                        safe_view_count: { $convert: { input: "$view_count", to: "int", onError: 0, onNull: 0 } },
                        safe_like_count: { $convert: { input: "$like_count", to: "int", onError: 0, onNull: 0 } },
                        safe_comment_count: { $convert: { input: "$comment_count", to: "int", onError: 0, onNull: 0 } },
                        safe_share_count: { $convert: { input: "$share_count", to: "int", onError: 0, onNull: 0 } }
                    }
                },
                {
                    $group: {
                        _id: "$username",
                        total_videos: { $sum: 1 },
                        total_views: { $sum: "$safe_view_count" },
                        total_likes: { $sum: "$safe_like_count" },
                        total_comments: { $sum: "$safe_comment_count" },
                        total_shares: { $sum: "$safe_share_count" },
                        avg_views: { $avg: "$safe_view_count" },
                        avg_likes: { $avg: "$safe_like_count" },
                        avg_comments: { $avg: "$safe_comment_count" },
                        avg_shares: { $avg: "$safe_share_count" },
                        min_views: { $min: "$safe_view_count" },
                        max_views: { $max: "$safe_view_count" },
                        min_likes: { $min: "$safe_like_count" },
                        max_likes: { $max: "$safe_like_count" },
                        min_comments: { $min: "$safe_comment_count" },
                        max_comments: { $max: "$safe_comment_count" },
                        min_shares: { $min: "$safe_share_count" },
                        max_shares: { $max: "$safe_share_count" }
                    }
                },
                {
                    $lookup: {
                        from: "tiktok_user_info",
                        localField: "_id",
                        foreignField: "username",
                        as: "user_info"
                    }
                },
                {
                    $project: {
                        username: "$_id",
                        is_verified: { $arrayElemAt: ["$user_info.is_verified", 0] },
                        follower_count: { $convert: { input: { $arrayElemAt: ["$user_info.follower_count", 0] }, to: "int", onError: 1, onNull: 1 } },
                        total_videos: 1,
                        total_views: 1,
                        total_likes: 1,
                        total_comments: 1,
                        total_shares: 1,
                        avg_views: 1,
                        avg_likes: 1,
                        avg_comments: 1,
                        avg_shares: 1,
                        min_views: 1,
                        max_views: 1,
                        min_likes: 1,
                        max_likes: 1,
                        min_comments: 1,
                        max_comments: 1,
                        min_shares: 1,
                        max_shares: 1,
                        views_per_follower: { 
                            $divide: ["$total_views", { $max: [1, "$follower_count"] }]
                        },
                        likes_per_follower: { 
                            $divide: ["$total_likes", { $max: [1, "$follower_count"] }]
                        },
                        engagement_rate: {
                            $multiply: [
                                {
                                    $divide: [
                                        { $add: ["$total_likes", "$total_comments", "$total_shares"] },
                                        { $max: [1, "$total_views"] }
                                    ]
                                },
                                100
                            ]
                        }
                    }
                },
                { $sort: { engagement_rate: -1 } }
            ],
            
            // Get usernames from videos collection to use later
            "video_usernames": [
                { $group: { _id: "$username" } },
                { $project: { _id: 0, username: "$_id" } }
            ]
        }
    },
    
    // Next, get all usernames from tiktok_user_info and identify missing ones
    {
        $lookup: {
            from: "tiktok_user_info",
            pipeline: [
                { $project: { _id: 0, username: 1, follower_count: 1, is_verified: 1 } }
            ],
            as: "all_influencers"
        }
    },
    
    // Final output
    {
        $project: {
            engagement_metrics: "$engagement_metrics",
            total_influencers: { $size: "$engagement_metrics" },
            missing_influencers: {
                $filter: {
                    input: "$all_influencers",
                    as: "influencer",
                    cond: { 
                        $not: [
                            { $in: ["$$influencer.username", "$video_usernames.username"] }
                        ]
                    }
                }
            },
            total_missing: {
                $subtract: [
                    { $size: "$all_influencers" },
                    { $size: "$engagement_metrics" }
                ]
            }
        }
    }
]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/user_videos_comprehensive_analysis.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);
