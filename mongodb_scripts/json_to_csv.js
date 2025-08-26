const fs = require('fs');
const path = require('path');

// Helper function to convert JSON to CSV
function jsonToCSV(jsonData, headers) {
    // Create CSV header
    let csv = headers.join(',') + '\n';
    
    // Add data rows
    jsonData.forEach(item => {
        const row = headers.map(header => {
            const value = getNestedValue(item, header);
            
            // Handle different value types
            if (value === null || value === undefined) {
                return '';
            } else if (typeof value === 'object') {
                return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
            } else if (typeof value === 'string' && value.includes(',')) {
                return `"${value.replace(/"/g, '""')}"`;
            } else {
                return value;
            }
        }).join(',');
        csv += row + '\n';
    });
    
    return csv;
}

// Helper function to get nested values using dot notation
function getNestedValue(obj, path) {
    const keys = path.split('.');
    return keys.reduce((o, key) => (o && o[key] !== undefined) ? o[key] : '', obj);
}

// Process all JSON files
function processAllFiles() {
    const outputDir = path.join(__dirname, 'output');
    const platforms = ['tiktok', 'youtube'];
    
    // Create output directories if they don't exist
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
        console.log(`Created directory: ${outputDir}`);
    }
    
    platforms.forEach(platform => {
        const platformDir = path.join(outputDir, platform);
        
        // Create platform directories if they don't exist
        if (!fs.existsSync(platformDir)) {
            fs.mkdirSync(platformDir);
            console.log(`Created directory: ${platformDir}`);
        }
        
        // Read all JSON files in platform directory
        try {
            const files = fs.readdirSync(platformDir)
                          .filter(file => file.endsWith('.json'));
            
            files.forEach(file => {
                try {
                    const filePath = path.join(platformDir, file);
                    const jsonData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                    
                    // Skip empty arrays
                    if (!Array.isArray(jsonData) || jsonData.length === 0) {
                        console.log(`Skipping empty file: ${filePath}`);
                        return;
                    }
                    
                    // Determine headers based on file structure
                    let headers = [];
                    let csv = '';
                    
                    // Define the CSV structure based on the file
                    const baseName = path.basename(file, '.json');
                    
                    if (baseName === 'likes_count' || baseName === 'followers_count') {
                        // Simple two-column files
                        headers = ['username', baseName.replace('_count', '_count')];
                        
                        // Make sure all values are correct
                        const flatData = jsonData.map(item => ({
                            username: item.username,
                            [baseName]: item[baseName] !== undefined ? item[baseName] : 0
                        }));
                        
                        csv = jsonToCSV(flatData, headers);
                    } 
                    else if (baseName === 'video_duration') {
                        headers = ['category', 'count', 'percentage'];
                        csv = jsonToCSV(jsonData, headers);
                    }
                    else if (baseName === 'user_video_duration') {
                        // Fix for user_video_duration.csv
                        csv = 'username,total_videos,short_videos,short_percentage,mid_videos,mid_percentage,long_videos,long_percentage,extra_long_videos,extra_long_percentage\n';
                        
                        jsonData.forEach(item => {
                            const username = item.username;
                            const totalVideos = item.total_videos;
                            
                            // Initialize counters
                            let shortCount = 0, midCount = 0, longCount = 0, extraLongCount = 0;
                            
                            // Process duration categories
                            item.duration_categories.forEach(category => {
                                if (category.category === 'SHORT (<15s)') shortCount = category.count;
                                else if (category.category === 'MID (15-60s)') midCount = category.count;
                                else if (category.category === 'LONG (1-5min)') longCount = category.count;
                                else if (category.category === 'EXTRA_LONG (>5min)') extraLongCount = category.count;
                            });
                            
                            // Calculate percentages
                            const shortPct = totalVideos > 0 ? ((shortCount / totalVideos) * 100).toFixed(2) : 0;
                            const midPct = totalVideos > 0 ? ((midCount / totalVideos) * 100).toFixed(2) : 0;
                            const longPct = totalVideos > 0 ? ((longCount / totalVideos) * 100).toFixed(2) : 0;
                            const extraLongPct = totalVideos > 0 ? ((extraLongCount / totalVideos) * 100).toFixed(2) : 0;
                            
                            csv += `${username},${totalVideos},${shortCount},${shortPct},${midCount},${midPct},${longCount},${longPct},${extraLongCount},${extraLongPct}\n`;
                        });
                    }
                    else if (baseName === 'user_videos_comprehensive_analysis') {
                        // Fix for user_videos_comprehensive_analysis.csv
                        if (jsonData.length > 0 && jsonData[0].engagement_metrics) {
                            csv = 'metric,value\n';
                            
                            // Extract key metrics
                            if (jsonData[0].total_influencers) {
                                csv += `Total Influencers,${jsonData[0].total_influencers}\n`;
                            }
                            
                            if (jsonData[0].missing_influencers) {
                                csv += `Missing Influencers,${jsonData[0].missing_influencers.length}\n`;
                            }
                            
                            if (jsonData[0].total_missing) {
                                csv += `Total Missing,${jsonData[0].total_missing}\n`;
                            }
                            
                            // Extract engagement metrics if available
                            if (jsonData[0].engagement_metrics && typeof jsonData[0].engagement_metrics === 'object') {
                                Object.entries(jsonData[0].engagement_metrics).forEach(([key, value]) => {
                                    csv += `${key.replace(/_/g, ' ')},${value}\n`;
                                });
                            }
                        } else {
                            csv = 'Unable to parse comprehensive analysis data\n';
                        }
                    }
                    else if (baseName === 'user_verification') {
                        // This is a special case with a different structure
                        const data = jsonData[0];
                        csv = 'Status,Count,Percentage\n' +
                            `Verified,${data['Verified Count']},${data['Verified Percentage'].toFixed(2)}\n` +
                            `Non-Verified,${data['Non-Verified Count']},${data['Non-Verified Percentage'].toFixed(2)}\n`;
                    }
                    else if (baseName === 'user_popularity_engagement') {
                        headers = ['username', 'follower_count', 'likes_count', 'video_count', 'engagement_rate', 'popularity_score'];
                        csv = jsonToCSV(jsonData, headers);
                    }
                    else if (baseName === 'min_max_avg') {
                        headers = ['username', 'totalVideos', 'avgViewCount', 'minViewCount', 'maxViewCount', 
                                'avgLikeCount', 'minLikeCount', 'maxLikeCount',
                                'avgCommentCount', 'minCommentCount', 'maxCommentCount',
                                'avgShareCount', 'minShareCount', 'maxShareCount'];
                        csv = jsonToCSV(jsonData, headers);
                    }
                    else if (baseName === 'compare_videos') {
                        headers = ['username', 'declared_video_count', 'actual_video_count'];
                        csv = jsonToCSV(jsonData, headers);
                    }
                    else if (baseName === 'video_field_presence_stats') {
                        // Flatten the data
                        const data = jsonData[0];
                        headers = Object.keys(data);
                        const flatData = [data];
                        csv = jsonToCSV(flatData, headers);
                    }
                    else if (baseName === 'channel_by_topic_category') {
                        // Clean up the category URLs
                        headers = ['category_name', 'channelCount', 'avgViews', 'avgSubs', 'totalVideos'];
                        
                        const cleanedData = jsonData.map(item => {
                            // Extract category name from URL
                            const categoryUrl = item.category;
                            const categoryName = categoryUrl.split('/').pop().replace(/_/g, ' ');
                            
                            return {
                                category_name: categoryName,
                                channelCount: item.channelCount,
                                avgViews: item.avgViews,
                                avgSubs: item.avgSubs,
                                totalVideos: item.totalVideos
                            };
                        });
                        
                        csv = jsonToCSV(cleanedData, headers);
                    }
                    else if (baseName === 'video_comments_stats' || baseName === 'video_replies_stats') {
                        // These have more complex structures, extract key metrics
                        if (platform === 'youtube') {
                            // Define headers for YouTube stats
                            csv = 'channel_id,channel_title,total_comments,total_likes,avg_comments_per_video,max_comments_per_video\n';
                            
                            jsonData.forEach(item => {
                                const channelId = item.channel_id;
                                const channelTitle = item.channel_title || 'Unknown';
                                const stats = item.comments_stats || item.replies_stats;
                                
                                csv += `${channelId},"${channelTitle}",${stats.total_comments || stats.total_replies},` +
                                    `${stats.total_likes || stats.total_likes_on_replies},` +
                                    `${stats.comments_per_video?.avg || stats.per_comment_metrics?.replies?.avg},` +
                                    `${stats.comments_per_video?.max || stats.per_comment_metrics?.replies?.max}\n`;
                            });
                        } else {
                            // Define headers for TikTok stats
                            headers = ['username', 'total_videos_with_comments', 'total_comments', 'total_likes', 'total_replies'];
                            
                            // Flatten the data
                            const flatData = jsonData.map(item => ({
                                username: item.username,
                                total_videos_with_comments: item.stats.total_videos_with_comments,
                                total_comments: item.stats.total_comments,
                                total_likes: item.stats.total_likes,
                                total_replies: item.stats.total_replies
                            }));
                            
                            csv = jsonToCSV(flatData, headers);
                        }
                    }
                    else if (baseName === 'channel_basic_statistics') {
                        headers = ['Channel', 'Views', 'Subscribers', 'Videos', 'Views Per Video', 'Views Per Subscriber'];
                        csv = jsonToCSV(jsonData, headers);
                    }
                    else if (baseName === 'channel_by_country') {
                        headers = ['Country', 'Channel Count', 'Total Views', 'Total Subscribers'];
                        csv = jsonToCSV(jsonData, headers);
                    }
                    else {
                        // Default handling for other files
                        const sampleItem = jsonData[0];
                        headers = Object.keys(sampleItem);
                        csv = jsonToCSV(jsonData, headers);
                    }
                    
                    // Write CSV to file
                    const csvFilePath = path.join(platformDir, baseName + '.csv');
                    fs.writeFileSync(csvFilePath, csv);
                    console.log(`Converted ${filePath} to CSV at ${csvFilePath}`);
                    
                } catch (err) {
                    console.error(`Error processing ${file}: ${err.message}`);
                }
            });
        } catch (err) {
            console.error(`Warning: Could not process directory ${platformDir}: ${err.message}`);
            console.log(`Make sure to put your JSON files in ${platformDir}`);
        }
    });
}

// Run the conversion
processAllFiles();
console.log('All JSON files have been converted to CSV!'); 