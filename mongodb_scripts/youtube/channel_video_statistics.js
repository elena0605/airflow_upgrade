var results = [];
db.youtube_channel_videos.aggregate([
    // Stage 1: Group videos by channel to get accurate counts
    {
      $group: {
        _id: "$channel_id",
        channelTitle: { $first: "$channel_title" },
        videoCount: { $sum: 1 },
        totalViews: { $sum: { $toDouble: "$view_count" } },
        avgViews: { $avg: { $toDouble: "$view_count" } },
        maxViews: { $max: { $toDouble: "$view_count" } },
        minViews: { $min: { $toDouble: "$view_count" } },
        totalLikes: { $sum: { $toDouble: "$like_count" } },
        avgLikes: { $avg: { $toDouble: "$like_count" } },
        maxLikes: { $max: { $toDouble: "$like_count" } },
        minLikes: { $min: { $toDouble: "$like_count" } },
        totalComments: { $sum: { $toDouble: "$comment_count" } },
        avgComments: { $avg: { $toDouble: "$comment_count" } },
        maxComments: { $max: { $toDouble: "$comment_count" } },
        minComments: { $min: { $toDouble: "$comment_count" } }
      }
    },
    
    // Stage 2: Add a trimmed channel ID field
    {
      $addFields: {
        channelIdTrimmed: { $trim: { input: "$_id" } }
      }
    },
    
    // Stage 3: Look up channel stats using original ID
    {
      $lookup: {
        from: "youtube_channel_stats",
        localField: "_id",
        foreignField: "channel_id",
        as: "channelInfo"
      }
    },
    
    // Stage 4: For channels without stats, try looking up with trimmed ID
    {
      $lookup: {
        from: "youtube_channel_stats",
        localField: "channelIdTrimmed",
        foreignField: "channel_id",
        as: "channelInfoTrimmed"
      }
    },
    
    // Stage 5: Add fields for stats detection and sources
    {
      $addFields: {
        hasChannelStats: { $gt: [{ $size: "$channelInfo" }, 0] },
        hasChannelStatsWithTrimmedId: { $gt: [{ $size: "$channelInfoTrimmed" }, 0] },
        channelStats: { 
          $cond: [
            { $gt: [{ $size: "$channelInfo" }, 0] },
            { $arrayElemAt: ["$channelInfo", 0] },
            { $cond: [
              { $gt: [{ $size: "$channelInfoTrimmed" }, 0] },
              { $arrayElemAt: ["$channelInfoTrimmed", 0] },
              null
            ]}
          ]
        },
        idMatchStatus: {
          $cond: [
            { $gt: [{ $size: "$channelInfo" }, 0] },
            "Exact Match",
            { $cond: [
              { $gt: [{ $size: "$channelInfoTrimmed" }, 0] },
              "Matched After Trimming",
              "No Match"
            ]}
          ]
        }
      }
    },
    
    // Stage 6: Project the final fields
    {
      $project: {
        _id: 0,
        channelId: "$_id",
        channelIdTrimmed: 1,
        channelTitle: 1,
        fetchedVideosFor2024: "$videoCount",
        totalVideosForChannel: { 
          $cond: [
            { $or: ["$hasChannelStats", "$hasChannelStatsWithTrimmedId"] }, 
            { $toDouble: "$channelStats.video_count" }, 
            null
          ]
        },
        subscriberCount: { 
          $cond: [
            { $or: ["$hasChannelStats", "$hasChannelStatsWithTrimmedId"] },
            { $toDouble: "$channelStats.subscriber_count" },
            null
          ]
        },
        hasChannelStats: { $or: ["$hasChannelStats", "$hasChannelStatsWithTrimmedId"] },
        idMatchStatus: 1,
        videoStats: {
          views: {
            total: "$totalViews",
            average: { $round: ["$avgViews", 2] },
            max: "$maxViews",
            min: "$minViews"
          },
          likes: { 
            total: "$totalLikes",
            average: { $round: ["$avgLikes", 2] },
            max: "$maxLikes",
            min: "$minLikes"
          },
          comments: { 
            total: "$totalComments",
            average: { $round: ["$avgComments", 2] },
            max: "$maxComments",
            min: "$minComments"
          }
        }
      }
    },
    
    // Stage 6.5: Sort channels by video count (descending)
    {
      $sort: { fetchedVideosFor2024: -1 }
    },
    
    // Stage 7: Group for overall statistics
    {
      $group: {
        _id: null,
        totalVideos: { $sum: "$fetchedVideosFor2024" },
        uniqueChannels: { $sum: 1 },
        channelsWithExactMatch: { $sum: { $cond: [{ $eq: ["$idMatchStatus", "Exact Match"] }, 1, 0] } },
        channelsWithTrimmedMatch: { $sum: { $cond: [{ $eq: ["$idMatchStatus", "Matched After Trimming"] }, 1, 0] } },
        channelsWithNoMatch: { $sum: { $cond: [{ $eq: ["$idMatchStatus", "No Match"] }, 1, 0] } },
        allChannelDetails: { $push: "$$ROOT" }
      }
    },
    
    // Stage 8: Find channels in youtube_channel_stats that don't have videos
    {
      $lookup: {
        from: "youtube_channel_stats",
        pipeline: [
          // Subpipeline to find channels without videos
          {
            $lookup: {
              from: "youtube_channel_videos",
              let: { 
                channel_id: "$channel_id",
                channel_id_trimmed: { $trim: { input: "$channel_id" } }
              },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $or: [
                        { $eq: ["$channel_id", "$$channel_id"] },
                        { $eq: [{ $trim: { input: "$channel_id" } }, "$$channel_id_trimmed"] }
                      ]
                    }
                  }
                }
              ],
              as: "videos"
            }
          },
          {
            $match: {
              videos: { $size: 0 }
            }
          },
          {
            $project: {
              _id: 0,
              channel_id: 1,
              title: 1,
              declared_video_count: "$video_count",
              subscriber_count: 1
            }
          }
        ],
        as: "channelsWithoutVideos"
      }
    },
    
    // Stage 9: Final projection
    {
      $project: {
        _id: 0,
        totalVideos: 1,
        uniqueChannels: 1,
        totalChannelsInStats: { $add: ["$uniqueChannels", { $size: "$channelsWithoutVideos" }] },
        channelsWithExactMatch: 1,
        channelsWithTrimmedMatch: 1,
        channelsWithNoMatch: 1,
        channelsWithoutVideos: 1,
        channelsMatchedAfterTrimming: {
          $filter: {
            input: "$allChannelDetails",
            as: "channel",
            cond: { $eq: ["$$channel.idMatchStatus", "Matched After Trimming"] }
          }
        },
        channelsWithNoMatch: {
          $filter: {
            input: "$allChannelDetails",
            as: "channel",
            cond: { $eq: ["$$channel.idMatchStatus", "No Match"] }
          }
        },
        channelStats: {
          $filter: {
            input: "$allChannelDetails",
            as: "channel",
            cond: { $ne: ["$$channel.idMatchStatus", "No Match"] }
          }
        }
      }
    }
    ]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/channel_video_statistics.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));