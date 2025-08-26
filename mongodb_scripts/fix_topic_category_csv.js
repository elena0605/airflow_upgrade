const fs = require('fs');
const path = require('path');

// Main function to fix the channel_video_by_topic_category.csv
function fixTopicCategoryCSV() {
    console.log('Starting to fix channel_video_by_topic_category.csv...');
    
    // Read the source file as text
    const sourceFile = path.join(__dirname, 'output/youtube/channel_video_by_topic_category.csv');
    let fileContent;
    
    try {
        fileContent = fs.readFileSync(sourceFile, 'utf8');
        console.log('Successfully read the source file');
    } catch (err) {
        console.error(`Error reading source file: ${err.message}`);
        return;
    }
    
    // Parse the content to extract individual channel records
    const channelMap = new Map();
    
    // First clean up lines and try to identify complete records
    const lines = fileContent.split('\n').filter(line => line.trim());
    let headerLine = lines[0] || '';
    
    // If the first line looks like a header, process it
    if (headerLine.includes('channelId') && headerLine.includes('channelTitle')) {
        console.log('Found header line');
        
        // Process each data line
        let currentChannelId = '';
        let currentRecord = {};
        
        for (let i = 1; i < lines.length; i++) {
            const line = lines[i];
            
            // Check if this is a new record (starts with a channel ID)
            if (line.match(/^UC[a-zA-Z0-9_-]+/)) {
                // If we were working on a previous record, finalize it
                if (currentChannelId && Object.keys(currentRecord).length > 0) {
                    channelMap.set(currentChannelId, { ...currentRecord });
                }
                
                // Start new record
                const parts = line.split('\t');
                currentChannelId = parts[0];
                
                if (parts.length >= 5) {
                    currentRecord = {
                        channelId: parts[0],
                        channelTitle: parts[1],
                        totalVideos: parts[2],
                        channelStats: tryParseJSON(parts[3]),
                        topicCategories: tryParseJSON(parts[4])
                    };
                } else {
                    currentRecord = {
                        channelId: parts[0],
                        channelTitle: parts[1] || '',
                        totalVideos: parts[2] || '',
                        channelStats: {},
                        topicCategories: []
                    };
                }
            } else if (currentChannelId) {
                // This is a continuation line for the current record
                // Try to extract any topic category JSON that might be split
                const potentialJSON = line.trim();
                
                if (potentialJSON.startsWith('[') || potentialJSON.includes('topic')) {
                    // This looks like it might be part of the topicCategories
                    if (!currentRecord.rawTopicData) {
                        currentRecord.rawTopicData = '';
                    }
                    currentRecord.rawTopicData += potentialJSON;
                }
            }
        }
        
        // Don't forget the last record
        if (currentChannelId && Object.keys(currentRecord).length > 0) {
            channelMap.set(currentChannelId, { ...currentRecord });
        }
    }
    
    console.log(`Extracted ${channelMap.size} channel records`);
    
    // Process each channel record to properly parse JSON data
    const processedRecords = [];
    
    for (const [channelId, record] of channelMap.entries()) {
        const processedRecord = { ...record };
        
        // Try to parse channelStats if it's a string
        if (typeof processedRecord.channelStats === 'string') {
            processedRecord.channelStats = tryParseJSON(processedRecord.channelStats);
        }
        
        // Try to parse topicCategories
        if (!processedRecord.topicCategories || processedRecord.topicCategories.length === 0) {
            // Try to parse from rawTopicData
            if (processedRecord.rawTopicData) {
                try {
                    // Clean up the data - it might have incomplete JSON
                    let cleanedData = processedRecord.rawTopicData;
                    if (!cleanedData.endsWith(']')) {
                        cleanedData += ']';
                    }
                    if (!cleanedData.startsWith('[')) {
                        cleanedData = '[' + cleanedData;
                    }
                    
                    processedRecord.topicCategories = JSON.parse(cleanedData);
                } catch (e) {
                    console.log(`Could not parse topic data for ${channelId}: ${e.message}`);
                    processedRecord.topicCategories = [];
                }
            }
        }
        
        // Remove the raw data field
        delete processedRecord.rawTopicData;
        
        processedRecords.push(processedRecord);
    }
    
    // Create a new CSV file with the processed data
    const outputFile = path.join(__dirname, 'channel_video_by_topic_category.csv');
    
    try {
        // Create the CSV content
        let csvContent = 'channelId,channelTitle,totalVideos,totalViews,totalLikes,totalComments,topic,videoCount,percentageOfChannelVideos,totalTopicViews,avgTopicViews,maxTopicViews,minTopicViews,totalTopicLikes,avgTopicLikes,maxTopicLikes,minTopicLikes,totalTopicComments,avgTopicComments,maxTopicComments,minTopicComments\n';
        
        processedRecords.forEach(record => {
            const channelId = record.channelId;
            const channelTitle = record.channelTitle;
            const totalVideos = record.totalVideos;
            
            // Extract channel stats
            const channelStats = record.channelStats || {};
            const totalViews = channelStats.totalViews || '';
            const totalLikes = channelStats.totalLikes || '';
            const totalComments = channelStats.totalComments || '';
            
            // Process each topic category
            const topicCategories = record.topicCategories || [];
            
            if (topicCategories.length === 0) {
                // Add a row with just the channel info
                csvContent += `${channelId},${channelTitle},${totalVideos},${totalViews},${totalLikes},${totalComments},,,,,,,,,,,,,,\n`;
            } else {
                topicCategories.forEach(topic => {
                    const topicName = topic.topic || '';
                    const videoCount = topic.videoCount || '';
                    const percentage = topic.percentageOfChannelVideos || '';
                    
                    // Extract view stats
                    const viewStats = topic.viewStats || {};
                    const totalTopicViews = viewStats.total || '';
                    const avgTopicViews = viewStats.average || '';
                    const maxTopicViews = viewStats.max || '';
                    const minTopicViews = viewStats.min || '';
                    
                    // Extract like stats
                    const likeStats = topic.likeStats || {};
                    const totalTopicLikes = likeStats.total || '';
                    const avgTopicLikes = likeStats.average || '';
                    const maxTopicLikes = likeStats.max || '';
                    const minTopicLikes = likeStats.min || '';
                    
                    // Extract comment stats
                    const commentStats = topic.commentStats || {};
                    const totalTopicComments = commentStats.total || '';
                    const avgTopicComments = commentStats.average || '';
                    const maxTopicComments = commentStats.max || '';
                    const minTopicComments = commentStats.min || '';
                    
                    // Add a row for this topic
                    csvContent += `${channelId},${channelTitle},${totalVideos},${totalViews},${totalLikes},${totalComments},${topicName},${videoCount},${percentage},${totalTopicViews},${avgTopicViews},${maxTopicViews},${minTopicViews},${totalTopicLikes},${avgTopicLikes},${maxTopicLikes},${minTopicLikes},${totalTopicComments},${avgTopicComments},${maxTopicComments},${minTopicComments}\n`;
                });
            }
        });
        
        // Write the file
        fs.writeFileSync(outputFile, csvContent);
        console.log(`Successfully wrote fixed data to ${outputFile}`);
        
    } catch (err) {
        console.error(`Error writing output file: ${err.message}`);
    }
}

// Helper function to safely parse JSON
function tryParseJSON(jsonString) {
    if (!jsonString || typeof jsonString !== 'string') {
        return {};
    }
    
    try {
        return JSON.parse(jsonString);
    } catch (e) {
        // If it fails, try to clean up the string
        try {
            // Handle incomplete JSON by adding missing brackets
            let cleanedString = jsonString;
            
            // Count opening and closing brackets to see if they match
            const openBrackets = (cleanedString.match(/\[/g) || []).length;
            const closeBrackets = (cleanedString.match(/\]/g) || []).length;
            
            // Add missing closing brackets
            if (openBrackets > closeBrackets) {
                for (let i = 0; i < openBrackets - closeBrackets; i++) {
                    cleanedString += ']';
                }
            }
            
            return JSON.parse(cleanedString);
        } catch (e2) {
            console.log(`Could not parse JSON: ${jsonString.substring(0, 100)}...`);
            return {};
        }
    }
}

// Run the fix function
fixTopicCategoryCSV(); 