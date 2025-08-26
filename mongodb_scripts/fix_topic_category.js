const fs = require('fs');
const path = require('path');

// Read and parse a CSV file
function readCSV(filePath) {
    try {
        console.log(`Reading file: ${filePath}`);
        const content = fs.readFileSync(filePath, 'utf8');
        const lines = content.split('\n').filter(line => line.trim());
        
        if (lines.length === 0) {
            console.log(`Empty file: ${filePath}`);
            return [];
        }
        
        const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
        const results = [];
        
        for (let i = 1; i < lines.length; i++) {
            const values = parseCSVLine(lines[i]);
            if (values.length !== headers.length) {
                console.log(`Skipping line ${i} in ${filePath} - column count mismatch`);
                continue;
            }
            
            const obj = {};
            headers.forEach((header, index) => {
                obj[header] = values[index] !== undefined ? values[index] : '';
            });
            
            results.push(obj);
        }
        
        return { headers, data: results };
    } catch (err) {
        console.error(`Error reading ${filePath}: ${err.message}`);
        return { headers: [], data: [] };
    }
}

// Parse a CSV line with proper quote handling
function parseCSVLine(line) {
    const values = [];
    let inQuotes = false;
    let currentValue = '';
    
    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        
        if (char === '"' && (i === 0 || line[i-1] !== '\\')) {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            values.push(currentValue.trim().replace(/^"|"$/g, ''));
            currentValue = '';
        } else {
            currentValue += char;
        }
    }
    
    values.push(currentValue.trim().replace(/^"|"$/g, ''));
    return values;
}

// Convert string to number if possible
function convertToNumber(value) {
    if (value === undefined || value === null || value === '') return '';
    const num = Number(value);
    return isNaN(num) ? value : num;
}

// Write data to CSV
function writeCSV(filePath, data, headers) {
    console.log(`Writing ${data.length} rows to ${filePath}`);
    let csvContent = headers.join(',') + '\n';
    
    data.forEach(row => {
        const rowValues = headers.map(header => {
            const value = row[header] !== undefined ? row[header] : '';
            return typeof value === 'string' && value.includes(',') ? `"${value}"` : value;
        });
        csvContent += rowValues.join(',') + '\n';
    });
    
    fs.writeFileSync(filePath, csvContent);
    console.log(`Successfully wrote ${data.length} rows to ${filePath}`);
}

// Main function to fix topic category CSV
function fixTopicCategoryCSV() {
    console.log('Starting to fix channel_video_by_topic_category.csv...');
    
    try {
        // Read the source CSV file
        const sourceFile = path.join(__dirname, 'output/youtube/channel_video_by_topic_category.csv');
        const { headers, data: sourceData } = readCSV(sourceFile);
        
        if (!sourceData || sourceData.length === 0) {
            console.error('Source CSV is empty or could not be read');
            return;
        }
        
        console.log(`Read ${sourceData.length} rows from source file`);
        
        // 1. Fix column headers if needed
        const fixedHeaders = headers.map(header => {
            // Remove any non-standard characters from headers
            return header.replace(/[^\w\s_]/g, '').trim();
        });
        
        // 2. Group by channel and topic category
        const channelTopicMap = new Map();
        
        sourceData.forEach(row => {
            // Extract channel ID or title for grouping
            const channelId = row.channel_id || '';
            const channelTitle = row.channel_title || '';
            const topicCategory = row.topic_category || row.category || '';
            
            // Skip if we can't identify the channel or topic
            if ((!channelId && !channelTitle) || !topicCategory) {
                return;
            }
            
            // Create a composite key
            const key = `${channelId || channelTitle}__${topicCategory}`;
            
            if (!channelTopicMap.has(key)) {
                channelTopicMap.set(key, {
                    channel_id: channelId,
                    channel_title: channelTitle,
                    topic_category: topicCategory,
                    video_count: 0,
                    avg_views: 0,
                    avg_likes: 0,
                    avg_comments: 0,
                    total_views: 0,
                    total_likes: 0,
                    total_comments: 0
                });
            }
            
            const entry = channelTopicMap.get(key);
            
            // Update numeric fields
            const videoCount = convertToNumber(row.video_count) || 0;
            const views = convertToNumber(row.views) || convertToNumber(row.total_views) || 0;
            const likes = convertToNumber(row.likes) || convertToNumber(row.total_likes) || 0;
            const comments = convertToNumber(row.comments) || convertToNumber(row.total_comments) || 0;
            
            entry.video_count += videoCount;
            entry.total_views += views;
            entry.total_likes += likes;
            entry.total_comments += comments;
            
            // Add any other fields that might be relevant
            Object.entries(row).forEach(([field, value]) => {
                if (!['channel_id', 'channel_title', 'topic_category', 'category', 
                     'video_count', 'views', 'total_views', 'likes', 'total_likes', 
                     'comments', 'total_comments'].includes(field)) {
                    if (value && (!entry[field] || entry[field] === '')) {
                        entry[field] = value;
                    }
                }
            });
        });
        
        // Calculate averages
        channelTopicMap.forEach(entry => {
            if (entry.video_count > 0) {
                entry.avg_views = entry.total_views / entry.video_count;
                entry.avg_likes = entry.total_likes / entry.video_count;
                entry.avg_comments = entry.total_comments / entry.video_count;
            }
        });
        
        // Convert map to array
        const fixedData = Array.from(channelTopicMap.values());
        
        // Sort by channel ID/title and topic category
        fixedData.sort((a, b) => {
            const channelA = a.channel_id || a.channel_title || '';
            const channelB = b.channel_id || b.channel_title || '';
            
            if (channelA < channelB) return -1;
            if (channelA > channelB) return 1;
            
            const topicA = a.topic_category || '';
            const topicB = b.topic_category || '';
            
            if (topicA < topicB) return -1;
            if (topicA > topicB) return 1;
            
            return 0;
        });
        
        // Prepare headers for output
        const outputHeaders = ['channel_id', 'channel_title', 'topic_category', 
                              'video_count', 'total_views', 'total_likes', 'total_comments',
                              'avg_views', 'avg_likes', 'avg_comments'];
        
        // Find additional headers
        const additionalHeaders = new Set();
        fixedData.forEach(entry => {
            Object.keys(entry).forEach(key => {
                if (!outputHeaders.includes(key)) {
                    additionalHeaders.add(key);
                }
            });
        });
        
        // Add additional headers in sorted order
        Array.from(additionalHeaders).sort().forEach(header => {
            outputHeaders.push(header);
        });
        
        // Write the fixed file
        const outputFile = path.join(__dirname, 'output/youtube/channel_video_by_topic_category_fixed.csv');
        writeCSV(outputFile, fixedData, outputHeaders);
        
        // Also create a copy in the main directory for easier access
        const mainOutputFile = path.join(__dirname, 'channel_video_by_topic_category.csv');
        writeCSV(mainOutputFile, fixedData, outputHeaders);
        
        console.log('Topic category CSV fix completed!');
        
    } catch (err) {
        console.error(`Error fixing topic category CSV: ${err.message}`);
    }
}

// Run the fix function
fixTopicCategoryCSV(); 