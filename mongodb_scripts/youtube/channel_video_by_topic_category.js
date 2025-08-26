var results = [];
db.youtube_channel_videos.aggregate([
    {
      $project: {
        video_id: 1,
        channel_id: { $trim: { input: { $ifNull: ["$channel_id", ""] } } },
        channel_title: 1,
        topic_categories: {
          $cond: [
            { $or: [{ $eq: ["$topic_categories", null] }, { $eq: ["$topic_categories", []] }] },
            ["Uncategorized"],
            "$topic_categories"
          ]
        },
        view_count: { $toDouble: { $ifNull: ["$view_count", "0"] } },
        like_count: { $toDouble: { $ifNull: ["$like_count", "0"] } },
        comment_count: { $toDouble: { $ifNull: ["$comment_count", "0"] } }
      }
    },
    
    {
      $group: {
        _id: "$channel_id",
        channelTitle: { $first: "$channel_title" },
        uniqueVideoCount: { $sum: 1 },
        channelTotalViews: { $sum: "$view_count" },
        channelTotalLikes: { $sum: "$like_count" },
        channelTotalComments: { $sum: "$comment_count" },
        videos: {
          $push: {
            video_id: "$video_id",
            topic_categories: "$topic_categories",
            view_count: "$view_count",
            like_count: "$like_count",
            comment_count: "$comment_count"
          }
        }
      }
    },
    
    { $unwind: "$videos" },
    { $unwind: "$videos.topic_categories" },
    
    {
      $group: {
        _id: {
          channel_id: "$_id",
          channelTitle: "$channelTitle",
          uniqueVideoCount: "$uniqueVideoCount",
          channelTotalViews: "$channelTotalViews",
          channelTotalLikes: "$channelTotalLikes",
          channelTotalComments: "$channelTotalComments",
          topic: "$videos.topic_categories"
        },
        videoCount: { $sum: 1 },
        totalViews: { $sum: "$videos.view_count" },
        totalLikes: { $sum: "$videos.like_count" },
        totalComments: { $sum: "$videos.comment_count" },
        avgViews: { $avg: "$videos.view_count" },
        avgLikes: { $avg: "$videos.like_count" },
        avgComments: { $avg: "$videos.comment_count" },
        maxViews: { $max: "$videos.view_count" },
        minViews: { $min: "$videos.view_count" },
        maxLikes: { $max: "$videos.like_count" },
        minLikes: { $min: "$videos.like_count" },
        maxComments: { $max: "$videos.comment_count" },
        minComments: { $min: "$videos.comment_count" }
      }
    },
    
    {
      $addFields: {
        formattedTopic: {
          $replaceAll: {
            input: "$_id.topic",
            find: "https://en.wikipedia.org/wiki/",
            replacement: ""
          }
        }
      }
    },
    
    {
      $group: {
        _id: {
          channel_id: "$_id.channel_id",
          channelTitle: "$_id.channelTitle"
        },
        uniqueVideoCount: { $first: "$_id.uniqueVideoCount" },
        channelTotalViews: { $first: "$_id.channelTotalViews" },
        channelTotalLikes: { $first: "$_id.channelTotalLikes" },
        channelTotalComments: { $first: "$_id.channelTotalComments" },
        topics: {
          $push: {
            topic: "$formattedTopic",
            videoCount: "$videoCount",
            viewStats: {
              total: "$totalViews",
              average: { $round: [{ $ifNull: ["$avgViews", 0] }, 2] },
              max: "$maxViews",
              min: "$minViews"
            },
            likeStats: {
              total: "$totalLikes",
              average: { $round: [{ $ifNull: ["$avgLikes", 0] }, 2] },
              max: "$maxLikes",
              min: "$minLikes"
            },
            commentStats: {
              total: "$totalComments",
              average: { $round: [{ $ifNull: ["$avgComments", 0] }, 2] },
              max: "$maxComments",
              min: "$minComments"
            }
          }
        }
      }
    },
    
    {
      $project: {
        _id: 0,
        channelId: "$_id.channel_id",
        channelTitle: "$_id.channelTitle",
        totalVideos: "$uniqueVideoCount",
        channelStats: {
          totalViews: "$channelTotalViews",
          totalLikes: "$channelTotalLikes",
          totalComments: "$channelTotalComments"
        },
        topicCategories: {
          $map: {
            input: "$topics",
            as: "topic",
            in: {
              topic: "$$topic.topic",
              videoCount: "$$topic.videoCount",
              percentageOfChannelVideos: {
                $round: [{ $multiply: [{ $divide: ["$$topic.videoCount", "$uniqueVideoCount"] }, 100] }, 2]
              },
              viewStats: "$$topic.viewStats",
              likeStats: "$$topic.likeStats",
              commentStats: "$$topic.commentStats"
            }
          }
        }
      }
    },
    
    { $sort: { totalVideos: -1 } }
    ]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/channel_video_by_topic_category.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);