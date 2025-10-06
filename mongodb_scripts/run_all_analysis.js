// Master script to run all MongoDB analysis scripts against remote database
// Usage: mongosh "your-connection-string" --file run_all_analysis.js

// Configuration - Auto-detect script directory
const SCRIPT_DIR = __dirname || "/Users/bojansimoski/dev/airflow_upgrade/mongodb_scripts";
const OUTPUT_DIR = SCRIPT_DIR + "/output";

// Normalize hardcoded output base used inside individual scripts
const HARDCODED_OUTPUT_BASE = "/opt/airflow/mongodb_scripts/output";

// Ensure output directories exist and rewrite hardcoded paths on the fly
try {
    // Lazily create base output dir if missing
    if (!fs.existsSync(OUTPUT_DIR)) {
        // Recursive mkdir in case parent is missing
        fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    }
} catch (e) {
    print("⚠️  Could not create output directory:", OUTPUT_DIR, e.message);
}

// Monkey-patch fs.writeFileSync to redirect hardcoded output paths
try {
    const originalWriteFileSync = fs.writeFileSync;
    fs.writeFileSync = function(path, data, options) {
        if (typeof path === 'string') {
            // Replace the hardcoded base with our detected OUTPUT_DIR
            let rewrittenPath = path.replace(HARDCODED_OUTPUT_BASE, OUTPUT_DIR);
            // Ensure parent directory exists
            try {
                const lastSlash = rewrittenPath.lastIndexOf("/");
                if (lastSlash > 0) {
                    const dir = rewrittenPath.slice(0, lastSlash);
                    if (!fs.existsSync(dir)) {
                        fs.mkdirSync(dir, { recursive: true });
                    }
                }
            } catch (mkdirErr) {
                print("⚠️  Failed ensuring directory for:", rewrittenPath, mkdirErr.message);
            }
            return originalWriteFileSync.call(fs, rewrittenPath, data, options);
        }
        return originalWriteFileSync.apply(fs, arguments);
    };
} catch (patchErr) {
    print("⚠️  Failed to patch fs.writeFileSync:", patchErr.message);
}

print("=== Starting MongoDB Analysis Pipeline ===");
print("Timestamp:", new Date().toISOString());
print("Script directory:", SCRIPT_DIR);
print("Output directory:", OUTPUT_DIR);

// --- Connect to remote MongoDB first (if MONGODB_URI provided) ---
try {
    const uri = (typeof process !== 'undefined' && process.env && process.env.MONGODB_URI) ? process.env.MONGODB_URI : null;
    const explicitDbName = (typeof process !== 'undefined' && process.env && process.env.MONGODB_DB) ? process.env.MONGODB_DB : null;

    if (uri) {
        print("Connecting using MONGODB_URI (redacted for safety)...");
        const conn = new Mongo(uri);

        let selectedDb;
        if (explicitDbName) {
            selectedDb = conn.getDB(explicitDbName);
        } else {
            // Try to infer DB name from URI path; fallback to current 'db'
            try {
                const m = uri.match(/^mongodb(?:\+srv)?:\/\/[^\/]+\/(.*?)($|\?|#)/);
                const inferred = m && m[1] ? decodeURIComponent(m[1]) : null;
                selectedDb = inferred ? conn.getDB(inferred) : conn.getDB(db.getName());
            } catch (inferErr) {
                selectedDb = conn.getDB(db.getName());
            }
        }

        // Replace global db so downstream scripts use the remote connection
        globalThis.db = selectedDb;

        // Verify connection
        const ping = db.runCommand({ ping: 1 });
        if (ping.ok !== 1) {
            throw new Error("Ping failed: " + tojson(ping));
        }
        print("Connected to:", db.getName());
    } else {
        // No URI provided; verify existing connection
        const ping = db.runCommand({ ping: 1 });
        if (ping.ok !== 1) {
            throw new Error("No MONGODB_URI provided and current connection is not available. Set MONGODB_URI env var.");
        }
        print("Using existing mongosh connection to:", db.getName());
    }
} catch (connErr) {
    print("❌ Connection error:", connErr.message);
    print("Set MONGODB_URI (and optional MONGODB_DB) in the environment before running.");
    throw connErr;
}

// Analysis scripts to run (in order)
const TIKTOK_SCRIPTS = [
    "tiktok/followers_count.js",
    "tiktok/likes_count.js", 
    "tiktok/min_max_avg.js",
    "tiktok/user_verification.js",
    "tiktok/user_video_duration.js",
    "tiktok/video_duration.js",
    "tiktok/video_field_presence_stats.js",
    "tiktok/video_comments_stats.js",
    "tiktok/user_videos_additional_analysis.js",
    "tiktok/user_videos_comprehensive_analysis.js",
    "tiktok/user_popularity_engagement.js",
    "tiktok/compare_videos.js",
    "tiktok/export_tiktok_user_bios.js",
    "tiktok/merge_tiktok_bios.js"
];

const YOUTUBE_SCRIPTS = [
    "youtube/channel_basic_statistics.js",
    "youtube/channel_by_country.js",
    "youtube/channel_by_topic_category.js",
    "youtube/channel_empty_fields.js",
    "youtube/channel_video_by_topic_category.js",
    "youtube/channel_video_empty_fields.js",
    "youtube/channel_video_statistics.js",
    "youtube/video_comments_stats.js",
    "youtube/video_replies_stats.js",
    "youtube/export_youtube_user_descriptions.js",
    "youtube/merge_youtube_descriptions.js"
];

// Utility function to run a script with error handling
function runScript(scriptPath) {
    try {
        print(`\n--- Running: ${scriptPath} ---`);
        const startTime = new Date();
        
        // Load and execute the script
        load(scriptPath);
        
        const endTime = new Date();
        const duration = (endTime - startTime) / 1000;
        print(`✅ Completed: ${scriptPath} (${duration.toFixed(2)}s)`);
        
        return true;
    } catch (error) {
        print(`❌ Error in ${scriptPath}: ${error.message}`);
        print(`Stack trace: ${error.stack}`);
        return false;
    }
}

// Main execution function
function runAllAnalysis() {
    let successCount = 0;
    let totalCount = 0;
    
    // Run TikTok analysis
    print("\n=== TIKTOK ANALYSIS ===");
    TIKTOK_SCRIPTS.forEach(script => {
        totalCount++;
        if (runScript(SCRIPT_DIR + "/" + script)) {
            successCount++;
        }
    });
    
    // Run YouTube analysis  
    print("\n=== YOUTUBE ANALYSIS ===");
    YOUTUBE_SCRIPTS.forEach(script => {
        totalCount++;
        if (runScript(SCRIPT_DIR + "/" + script)) {
            successCount++;
        }
    });
    
    // Summary
    print("\n=== ANALYSIS COMPLETE ===");
    print(`Successfully completed: ${successCount}/${totalCount} scripts`);
    print(`End time: ${new Date().toISOString()}`);
    
    if (successCount === totalCount) {
        print("🎉 All analysis scripts completed successfully!");
    } else {
        print(`⚠️  ${totalCount - successCount} scripts failed. Check logs above.`);
    }
}

// Run the analysis
runAllAnalysis();
