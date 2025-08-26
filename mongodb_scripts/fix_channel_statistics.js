const fs = require('fs');
const path = require('path');

// Main function to fix the channel_video_statistics.csv
function fixChannelStatisticsCSV() {
    console.log('Starting to fix channel_video_statistics.csv...');
    
    // Read the source file as text
    const sourceFile = path.join(__dirname, 'output/youtube/channel_video_statistics.csv');
    let fileContent;
    
    try {
        fileContent = fs.readFileSync(sourceFile, 'utf8');
        console.log('Successfully read the source file');
    } catch (err) {
        console.error(`Error reading source file: ${err.message}`);
        return;
    }
    
    // Split the content into sections
    const lines = fileContent.split('\n');
    
    // Find the "Channel Details" section
    let channelDetailsIndex = -1;
    for (let i = 0; i < lines.length; i++) {
        if (lines[i].includes('Channel Details')) {
            channelDetailsIndex = i;
            break;
        }
    }
    
    if (channelDetailsIndex === -1) {
        console.error('Could not find Channel Details section');
        return;
    }
    
    // Extract the header and data rows from the Channel Details section
    const headerLine = lines[channelDetailsIndex + 1];
    if (!headerLine) {
        console.error('Could not find header line');
        return;
    }
    
    // Parse the header
    const headers = headerLine.split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    
    // Extract the channel data rows
    const channelData = [];
    
    for (let i = channelDetailsIndex + 2; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        // Process the line
        const values = parseCSVLine(line);
        if (values.length !== headers.length) {
            console.log(`Skipping line ${i} - column count mismatch`);
            continue;
        }
        
        // Create an object for this channel
        const channelObj = {};
        headers.forEach((header, index) => {
            // Clean up the values
            let value = values[index];
            if (header === 'Channel ID' || header === 'channel_id') {
                // Remove any whitespace from channel IDs
                value = value.trim();
            }
            channelObj[header] = value;
        });
        
        channelData.push(channelObj);
    }
    
    console.log(`Extracted ${channelData.length} channel records`);
    
    // Create a new CSV file with the cleaned data
    const outputFile = path.join(__dirname, 'channel_video_statistics.csv');
    
    try {
        // Create standardized headers
        const standardHeaders = [
            'channel_id',
            'channel_title',
            'video_count',
            'subscriber_count',
            'avg_views_per_video',
            'avg_likes_per_video',
            'avg_comments_per_video'
        ];
        
        // Map the existing headers to standard headers
        const headerMap = {
            'Channel ID': 'channel_id',
            'Channel Title': 'channel_title',
            'Video Count': 'video_count',
            'Subscriber Count': 'subscriber_count',
            'Avg Views Per Video': 'avg_views_per_video',
            'Avg Likes Per Video': 'avg_likes_per_video',
            'Avg Comments Per Video': 'avg_comments_per_video'
        };
        
        // Create the CSV content
        let csvContent = standardHeaders.join(',') + '\n';
        
        channelData.forEach(channel => {
            const rowValues = standardHeaders.map(header => {
                // Find the value using the header mapping or directly
                const value = channel[headerMap[header]] || channel[header] || '';
                return typeof value === 'string' && value.includes(',') ? `"${value}"` : value;
            });
            
            csvContent += rowValues.join(',') + '\n';
        });
        
        // Write the file
        fs.writeFileSync(outputFile, csvContent);
        console.log(`Successfully wrote fixed data to ${outputFile}`);
        
    } catch (err) {
        console.error(`Error writing output file: ${err.message}`);
    }
}

// Function to parse a CSV line with proper quote handling
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

// Run the fix function
fixChannelStatisticsCSV(); 