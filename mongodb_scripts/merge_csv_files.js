const fs = require('fs');
const path = require('path');

// Function to read a CSV file without dependencies
function readCSV(filePath) {
    return new Promise((resolve, reject) => {
        try {
            const content = fs.readFileSync(filePath, 'utf8');
            const lines = content.split('\n');
            
            // Extract headers
            const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
            
            // Process data rows
            const results = [];
            for (let i = 1; i < lines.length; i++) {
                if (!lines[i].trim()) continue; // Skip empty lines
                
                // Parse CSV line properly handling quotes
                const values = parseCSVLine(lines[i]);
                
                // Create object with headers as keys
                const obj = {};
                headers.forEach((header, index) => {
                    obj[header] = values[index] !== undefined ? values[index] : '';
                });
                
                results.push(obj);
            }
            
            resolve(results);
        } catch (err) {
            reject(err);
        }
    });
}

// Helper function to parse a CSV line handling quotes
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
    
    // Add the last value
    values.push(currentValue.trim().replace(/^"|"$/g, ''));
    
    return values;
}

// Convert string to number if possible
function convertToNumber(value) {
    if (value === undefined || value === null || value === '') return '';
    const num = Number(value);
    return isNaN(num) ? value : num;
}

// Function to write an array of objects to a CSV file
function writeCSV(filePath, data, headers) {
    if (!data || data.length === 0) {
        console.error(`No data to write to ${filePath}`);
        return;
    }

    // Create CSV header
    let csvContent = headers.join(',') + '\n';
    
    // Add data rows
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

// Function to merge TikTok CSV files
async function mergeTikTokFiles() {
    const tikTokDir = path.join(__dirname, 'output/tiktok');
    const outputFile = path.join(__dirname, 'tiktok_consolidated.csv');
    
    try {
        // Get all CSV files
        const files = fs.readdirSync(tikTokDir).filter(file => file.endsWith('.csv'));
        
        // Create a map to store data by username
        const userDataMap = new Map();
        
        // Process each file
        for (const file of files) {
            // Skip overall statistics files that don't have user-level data
            if (file === 'video_duration.csv' || file === 'video_field_presence_stats.csv') {
                continue;
            }
            
            const filePath = path.join(tikTokDir, file);
            
            try {
                const data = await readCSV(filePath);
                
                // Skip empty files
                if (!data || data.length === 0) {
                    console.log(`Skipping empty file: ${file}`);
                    continue;
                }
                
                console.log(`Processing ${file} with ${data.length} rows`);
                
                // Skip files without username column
                if (!data[0].hasOwnProperty('username')) {
                    console.log(`Skipping ${file} - no username column`);
                    continue;
                }
                
                // Add data to the user map
                data.forEach(row => {
                    const username = row.username;
                    if (!username) return;
                    
                    if (!userDataMap.has(username)) {
                        userDataMap.set(username, { username });
                    }
                    
                    const userData = userDataMap.get(username);
                    
                    // Add each field (convert numbers where appropriate)
                    Object.keys(row).forEach(key => {
                        if (key !== 'username') {
                            // Convert numeric values
                            if (/count|rate|percentage|total|avg|min|max|pct/.test(key)) {
                                userData[key] = convertToNumber(row[key]);
                            } else {
                                userData[key] = row[key];
                            }
                        }
                    });
                });
            } catch (err) {
                console.error(`Error processing file ${file}: ${err.message}`);
            }
        }
        
        // Convert map to array
        const allData = Array.from(userDataMap.values());
        
        // Write complete consolidated file
        const allHeaders = ['username'];
        allData.forEach(item => {
            Object.keys(item).forEach(key => {
                if (key !== 'username' && !allHeaders.includes(key)) {
                    allHeaders.push(key);
                }
            });
        });
        
        writeCSV(outputFile, allData, allHeaders);
        
    } catch (err) {
        console.error(`Error merging TikTok files: ${err.message}`);
    }
}

// Completely rewritten function to merge YouTube CSV files
async function mergeYouTubeFiles() {
    const youtubeDir = path.join(__dirname, 'output/youtube');
    const outputFile = path.join(__dirname, 'youtube_consolidated.csv');
    
    try {
        // Get all CSV files
        const files = fs.readdirSync(youtubeDir).filter(file => file.endsWith('.csv'));
        
        // First, load and process the main channel stats file
        const mainStatsFile = files.find(file => file === 'channel_stats.csv');
        let mainChannelData = [];
        
        if (mainStatsFile) {
            const filePath = path.join(youtubeDir, mainStatsFile);
            mainChannelData = await readCSV(filePath);
            console.log(`Loaded main channel stats file with ${mainChannelData.length} channels`);
        }
        
        // Create a consolidated map with channel_id as key
        const consolidatedChannels = new Map();
        
        // First add all channels from the main stats file
        if (mainChannelData.length > 0) {
            mainChannelData.forEach(channel => {
                const channelId = channel.channel_id || '';
                const channelTitle = channel.channel_title || channel.Channel || '';
                
                if (channelId || channelTitle) {
                    const key = channelId || channelTitle;
                    consolidatedChannels.set(key, {
                        channel_id: channelId,
                        channel_title: channelTitle,
                        ...channel
                    });
                }
            });
        }
        
        // Process each additional file and merge data
        for (const file of files) {
            if (file === 'channel_stats.csv') continue; // Already processed
            
            const filePath = path.join(youtubeDir, file);
            console.log(`Processing YouTube file: ${file}`);
            
            try {
                // Special handling for channel_video_statistics.csv
                let data = [];
                
                if (file === 'channel_video_statistics.csv') {
                    // Read the file content and extract channel data section
                    const content = fs.readFileSync(filePath, 'utf8');
                    
                    // Try to extract the Channel Details section
                    if (content.includes('Channel Details')) {
                        // Split by sections
                        const channelSectionMatch = content.match(/Channel Details.*?(?=Video Details|$)/s);
                        
                        if (channelSectionMatch) {
                            const channelSection = channelSectionMatch[0];
                            const lines = channelSection.split('\n');
                            
                            // Find the header line (usually contains 'Channel ID' or similar)
                            let headerIndex = -1;
                            for (let i = 0; i < lines.length; i++) {
                                if (lines[i].includes('Channel ID') || lines[i].includes('channel_id')) {
                                    headerIndex = i;
                                    break;
                                }
                            }
                            
                            if (headerIndex >= 0) {
                                const headers = lines[headerIndex].split(',').map(h => h.trim());
                                
                                // Process each data row
                                for (let i = headerIndex + 1; i < lines.length; i++) {
                                    if (!lines[i].trim() || lines[i].includes('Video Details')) break;
                                    
                                    const values = parseCSVLine(lines[i]);
                                    if (values.length !== headers.length) continue;
                                    
                                    const row = {};
                                    headers.forEach((header, idx) => {
                                        row[header] = values[idx];
                                    });
                                    
                                    data.push(row);
                                }
                            }
                        }
                    }
                } else {
                    // Normal CSV file processing
                    data = await readCSV(filePath);
                }
                
                if (!data || data.length === 0) {
                    console.log(`No data found in ${file}`);
                    continue;
                }
                
                console.log(`Processing ${data.length} rows from ${file}`);
                
                // Create a map from the current file for easier lookup
                const channelMap = new Map();
                
                // First group data by channel ID or title
                data.forEach(row => {
                    // Extract channel identifiers (handle different possible column names)
                    const channelId = row.channel_id || row['Channel ID'] || '';
                    const channelTitle = row.channel_title || row.Channel || row['Channel Title'] || '';
                    
                    if (!channelId && !channelTitle) return; // Skip if no identifier
                    
                    const key = channelId || channelTitle;
                    
                    if (!channelMap.has(key)) {
                        channelMap.set(key, {
                            channel_id: channelId,
                            channel_title: channelTitle,
                            data: []
                        });
                    }
                    
                    channelMap.get(key).data.push(row);
                });
                
                // Now merge data from this file into the consolidated map
                for (const [key, channelData] of channelMap.entries()) {
                    if (!consolidatedChannels.has(key)) {
                        // New channel, create entry
                        consolidatedChannels.set(key, {
                            channel_id: channelData.channel_id,
                            channel_title: channelData.channel_title
                        });
                    }
                    
                    const consolidatedChannel = consolidatedChannels.get(key);
                    
                    // Update missing channel_id or channel_title
                    if (!consolidatedChannel.channel_id && channelData.channel_id) {
                        consolidatedChannel.channel_id = channelData.channel_id;
                    }
                    if (!consolidatedChannel.channel_title && channelData.channel_title) {
                        consolidatedChannel.channel_title = channelData.channel_title;
                    }
                    
                    // Merge fields from all rows into the consolidated channel
                    for (const row of channelData.data) {
                        Object.entries(row).forEach(([field, value]) => {
                            // Skip channel identifier fields
                            if (field === 'channel_id' || field === 'Channel ID' || 
                                field === 'channel_title' || field === 'Channel' || 
                                field === 'Channel Title') {
                                return;
                            }
                            
                            // Skip empty values
                            if (value === undefined || value === null || value === '') {
                                return;
                            }
                            
                            // Convert numeric values
                            if (/count|views|subscribers|videos|likes|comments|rate|percentage|avg|min|max/i.test(field)) {
                                const numValue = convertToNumber(value);
                                
                                // Only update if the field doesn't exist or if we have a numeric value
                                if (numValue !== '' && 
                                    (consolidatedChannel[field] === undefined || 
                                     consolidatedChannel[field] === '' || 
                                     consolidatedChannel[field] === null)) {
                                    consolidatedChannel[field] = numValue;
                                }
                            } else {
                                // For non-numeric fields, only add if not already present
                                if (consolidatedChannel[field] === undefined || 
                                    consolidatedChannel[field] === '' || 
                                    consolidatedChannel[field] === null) {
                                    consolidatedChannel[field] = value;
                                }
                            }
                        });
                    }
                }
                
            } catch (err) {
                console.error(`Error processing file ${file}: ${err.message}`);
            }
        }
        
        console.log(`Successfully consolidated ${consolidatedChannels.size} YouTube channels`);
        
        // Convert map to array
        const finalData = Array.from(consolidatedChannels.values());
        
        // Create headers list
        let allHeaders = ['channel_id', 'channel_title']; // Primary keys first
        
        // Collect all other fields
        finalData.forEach(channel => {
            Object.keys(channel).forEach(field => {
                if (field !== 'channel_id' && field !== 'channel_title' && !allHeaders.includes(field)) {
                    allHeaders.push(field);
                }
            });
        });
        
        // Write to CSV
        writeCSV(outputFile, finalData, allHeaders);
        
    } catch (err) {
        console.error(`Error merging YouTube files: ${err.message}`);
    }
}

// Main function
async function mergeAllFiles() {
    console.log('Starting merge process...');
    
    await mergeTikTokFiles();
    await mergeYouTubeFiles();
    
    console.log('All merges completed!');
}

// Run the script
mergeAllFiles(); 