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
        
        return results;
    } catch (err) {
        console.error(`Error reading ${filePath}: ${err.message}`);
        return [];
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

// Main function to deduplicate YouTube channels
function deduplicateYoutubeChannels() {
    console.log('Starting YouTube channel deduplication...');
    
    try {
        // Read the source CSV file
        const sourceFile = path.join(__dirname, 'youtube_consolidated.csv');
        const sourceData = readCSV(sourceFile);
        
        if (!sourceData || sourceData.length === 0) {
            console.error('Source YouTube CSV is empty or could not be read');
            return;
        }
        
        console.log(`Read ${sourceData.length} rows from source file`);
        
        // Build a comprehensive channel ID to title mapping
        const idToTitle = new Map();
        const titleToId = new Map();
        
        // First pass: collect all explicit ID-title pairs
        sourceData.forEach(row => {
            const id = row.channel_id || '';
            const title = row.channel_title || '';
            
            if (id && title) {
                idToTitle.set(id, title);
                titleToId.set(title, id);
            }
        });
        
        console.log(`Identified ${idToTitle.size} explicit ID-title mappings`);
        
        // Prepare a map to store consolidated channels
        const channelMap = new Map();
        
        // Second pass: process all rows and merge duplicates
        sourceData.forEach(row => {
            let id = row.channel_id || '';
            let title = row.channel_title || '';
            
            // Try to resolve missing fields using our mappings
            if (id && !title && idToTitle.has(id)) {
                title = idToTitle.get(id);
                console.log(`Resolved title for ID ${id}: ${title}`);
            } else if (!id && title && titleToId.has(title)) {
                id = titleToId.get(title);
                console.log(`Resolved ID for title "${title}": ${id}`);
            }
            
            // Generate a unique key for this channel
            // If we have an ID, use it as the primary key
            // If no ID but we have a title, use the title
            let key = id || title;
            
            if (!key) {
                console.log('Skipping row with no identifiable information');
                return;
            }
            
            // Create or update channel entry
            if (!channelMap.has(key)) {
                channelMap.set(key, {
                    channel_id: id,
                    channel_title: title
                });
            }
            
            const channelData = channelMap.get(key);
            
            // Ensure we have the most complete ID and title
            if (id && !channelData.channel_id) channelData.channel_id = id;
            if (title && !channelData.channel_title) channelData.channel_title = title;
            
            // Merge all other fields
            Object.entries(row).forEach(([field, value]) => {
                // Skip ID and title fields as we've already handled them
                if (field === 'channel_id' || field === 'channel_title') {
                    return;
                }
                
                // Skip empty values
                if (value === undefined || value === null || value === '') {
                    return;
                }
                
                // For all other fields, prefer existing values if present
                if (channelData[field] === undefined || channelData[field] === '' || channelData[field] === null) {
                    channelData[field] = value;
                }
            });
        });
        
        console.log(`Consolidated to ${channelMap.size} unique channels`);
        
        // Convert map to array
        const consolidatedData = Array.from(channelMap.values());
        
        // Build headers list - start with identifiers
        const allHeaders = ['channel_id', 'channel_title'];
        
        // Add all other fields in a consistent order
        const otherFields = new Set();
        consolidatedData.forEach(channel => {
            Object.keys(channel).forEach(field => {
                if (field !== 'channel_id' && field !== 'channel_title') {
                    otherFields.add(field);
                }
            });
        });
        
        // Sort and add other fields
        Array.from(otherFields).sort().forEach(field => {
            allHeaders.push(field);
        });
        
        // Write the deduplicated file
        const outputFile = path.join(__dirname, 'youtube_consolidated.csv');
        writeCSV(outputFile, consolidatedData, allHeaders);
        
        console.log('YouTube channel deduplication completed!');
        
    } catch (err) {
        console.error(`Error deduplicating YouTube channels: ${err.message}`);
    }
}

// Run the deduplication
deduplicateYoutubeChannels(); 