const fs = require('fs');
const path = require('path');

// Fix the followers_count.csv file
function fixFollowersCount() {
    const filePath = path.join(__dirname, 'output/tiktok/followers_count.json');
    try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        let csv = 'username,followers_count\n';
        
        data.forEach(item => {
            csv += `${item.username},${item.follower_count}\n`;
        });
        
        fs.writeFileSync(path.join(__dirname, 'output/tiktok/followers_count.csv'), csv);
        console.log('Fixed followers_count.csv');
    } catch (err) {
        console.error(`Error fixing followers_count.csv: ${err.message}`);
    }
}

// Fix the user_videos_comprehensive_analysis.csv file
function fixUserVideosComprehensiveAnalysis() {
    const filePath = path.join(__dirname, 'output/tiktok/user_videos_additional_analysis.json');
    try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        
        // Create a proper summary format
        let csv = 'username,total_videos,videos_with_hashtags,videos_with_description,videos_with_voice,videos_with_all_fields,hashtags_pct,description_pct,voice_pct,all_fields_pct\n';
        
        data.forEach(item => {
            csv += `${item.username},${item.total_videos},${item.videos_with_hashtag_names},` +
                   `${item.videos_with_description},${item.videos_with_voice_to_text},` +
                   `${item.videos_with_all_fields},${item.pct_with_hashtag_names.toFixed(2)},` +
                   `${item.pct_with_description.toFixed(2)},${item.pct_with_voice_to_text.toFixed(2)},` +
                   `${item.pct_with_all_fields.toFixed(2)}\n`;
        });
        
        fs.writeFileSync(path.join(__dirname, 'output/tiktok/user_videos_comprehensive_analysis.csv'), csv);
        console.log('Fixed user_videos_comprehensive_analysis.csv');
    } catch (err) {
        console.error(`Error fixing user_videos_comprehensive_analysis.csv: ${err.message}`);
    }
}

// Fix the channel_video_empty_fields.csv file
function fixChannelVideoEmptyFields() {
    const filePath = path.join(__dirname, 'output/youtube/channel_video_empty_fields.json');
    try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        
        if (data.length > 0 && data[0].fieldAnalysis) {
            const stats = data[0].fieldAnalysis;
            let csv = 'Field,Present Count,Empty Count,Present Percentage\n';
            
            // Process each field
            const fields = [
                'videoTitle', 'videoId', 'publishedAt', 'channelId', 
                'videoDescription', 'channelTitle', 'viewCount', 
                'likeCount', 'commentCount', 'topicCategories', 'tags'
            ];
            
            fields.forEach(field => {
                const presentCount = stats[`${field}Count`] || 0;
                const emptyCount = stats[`${field}Empty`] || 0;
                const total = presentCount + emptyCount;
                const percentage = total > 0 ? ((presentCount / total) * 100).toFixed(2) : '0';
                
                csv += `${field},${presentCount},${emptyCount},${percentage}\n`;
            });
            
            fs.writeFileSync(path.join(__dirname, 'output/youtube/channel_video_empty_fields.csv'), csv);
            console.log('Fixed channel_video_empty_fields.csv');
        }
    } catch (err) {
        console.error(`Error fixing channel_video_empty_fields.csv: ${err.message}`);
    }
}

// Fix the channel_video_statistics.csv file
function fixChannelVideoStatistics() {
    const filePath = path.join(__dirname, 'output/youtube/channel_video_statistics.json');
    try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        
        if (data.length > 0) {
            // Create a summary table
            let csv = 'Metric,Value\n' +
                      `Total Videos,${data[0].totalVideos}\n` +
                      `Unique Channels,${data[0].uniqueChannels}\n` +
                      `Channels with Exact Match,${data[0].channelsWithExactMatch}\n` +
                      `Channels with Trimmed Match,${data[0].channelsWithTrimmedMatch}\n` +
                      `Channels with No Match,${data[0].channelsWithNoMatch}\n`;
            
            // Add a channels table
            csv += '\n\nChannel Details\n';
            csv += 'Channel Title,Channel ID,Video Count,Subscriber Count,Avg Views Per Video,Avg Likes Per Video,Avg Comments Per Video\n';
            
            // Add data for all channels
            const channels = data[0].channelStats || [];
            channels.forEach(channel => {
                const title = channel.channelTitle || 'Unknown';
                const id = channel.channelId || '';
                const videos = channel.fetchedVideosFor2024 || 0;
                const subs = channel.subscriberCount || 0;
                
                const avgViews = channel.videoStats?.views?.average?.toFixed(2) || 0;
                const avgLikes = channel.videoStats?.likes?.average?.toFixed(2) || 0; 
                const avgComments = channel.videoStats?.comments?.average?.toFixed(2) || 0;
                
                csv += `"${title}","${id}",${videos},${subs},${avgViews},${avgLikes},${avgComments}\n`;
            });
            
            fs.writeFileSync(path.join(__dirname, 'output/youtube/channel_video_statistics.csv'), csv);
            console.log('Fixed channel_video_statistics.csv');
        }
    } catch (err) {
        console.error(`Error fixing channel_video_statistics.csv: ${err.message}`);
    }
}

// Create output directories if they don't exist
function ensureDirectories() {
    const dirs = [
        path.join(__dirname, 'output'),
        path.join(__dirname, 'output/tiktok'),
        path.join(__dirname, 'output/youtube')
    ];
    
    dirs.forEach(dir => {
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
            console.log(`Created directory: ${dir}`);
        }
    });
}

// Main function to run all fixes
function runFixes() {
    ensureDirectories();
    fixFollowersCount();
    fixUserVideosComprehensiveAnalysis();
    fixChannelVideoEmptyFields();
    fixChannelVideoStatistics();
    console.log('All fixes completed!');
}

// Run the script
runFixes(); 