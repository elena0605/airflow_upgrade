// NOTE: Cosmos Mongo API has limited support for $lookup with pipeline.
// Rewrite: run two server-side aggregations and merge in JS.

// 1) Engagement metrics per user from videos
var engagement = db.tiktok_user_video.aggregate([
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
  }
]).toArray();

// 2) Basic user info from tiktok_user_info
var infos = db.tiktok_user_info.find({}, { _id: 0, username: 1, follower_count: 1, is_verified: 1 }).toArray();

// 3) Merge and compute derived metrics
var infoByUser = {};
infos.forEach(function(u) { infoByUser[u.username] = u; });

var merged = engagement.map(function(e) {
  var username = e._id;
  var info = infoByUser[username] || {};
  var follower = parseInt(info.follower_count, 10);
  follower = isNaN(follower) ? 1 : follower;
  var viewsPerFollower = e.total_views / Math.max(1, follower);
  var likesPerFollower = e.total_likes / Math.max(1, follower);
  var engagementRate = ((e.total_likes + e.total_comments + e.total_shares) / Math.max(1, e.total_views)) * 100;
  return {
    username: username,
    is_verified: info.is_verified,
    follower_count: follower,
    total_videos: e.total_videos,
    total_views: e.total_views,
    total_likes: e.total_likes,
    total_comments: e.total_comments,
    total_shares: e.total_shares,
    avg_views: e.avg_views,
    avg_likes: e.avg_likes,
    avg_comments: e.avg_comments,
    avg_shares: e.avg_shares,
    min_views: e.min_views,
    max_views: e.max_views,
    min_likes: e.min_likes,
    max_likes: e.max_likes,
    min_comments: e.min_comments,
    max_comments: e.max_comments,
    min_shares: e.min_shares,
    max_shares: e.max_shares,
    views_per_follower: viewsPerFollower,
    likes_per_follower: likesPerFollower,
    engagement_rate: engagementRate
  };
});

// 4) Identify influencers present in user_info but missing in videos
var presentUsers = {};
merged.forEach(function(m) { presentUsers[m.username] = true; });
var missingInfluencers = infos.filter(function(u) { return !presentUsers[u.username]; });

// 5) Final document
var results = [{
  engagement_metrics: merged.sort(function(a,b) { return b.engagement_rate - a.engagement_rate; }),
  total_influencers: merged.length,
  missing_influencers: missingInfluencers,
  total_missing: missingInfluencers.length
}];

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/tiktok/user_videos_comprehensive_analysis.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);
