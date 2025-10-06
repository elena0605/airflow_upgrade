// Simple MongoDB analysis entrypoint that:
// 1) Loads credentials from a .env file
// 2) Connects to remote MongoDB
// 3) Switches to the `rbl` database
// 4) Lists collections in `rbl`

// How to run:
//   mongosh --file /Users/bojansimoski/dev/airflow_upgrade/mongodb_scripts/mongodb_analysis.js

// Configuration
const SCRIPT_DIR = __dirname;
const PROJECT_ROOT = SCRIPT_DIR.replace(/\/mongodb_scripts$/, "");
const ENV_PATH = PROJECT_ROOT + "/.env";

function loadDotEnv(filePath) {
  const env = {};
  try {
    if (!fs.existsSync(filePath)) {
      print(".env not found at:", filePath);
      return env;
    }
    const content = fs.readFileSync(filePath, "utf8");
    content.split(/\r?\n/).forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) return;
      const eqIdx = trimmed.indexOf("=");
      if (eqIdx === -1) return;
      const key = trimmed.slice(0, eqIdx).trim();
      let value = trimmed.slice(eqIdx + 1).trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      }
      env[key] = value;
    });
  } catch (e) {
    print("⚠️  Failed to read .env:", e.message);
  }
  return env;
}

function buildMongoUri(env) {
  // Preferred: full URI provided
  const fullUri = env.MONGODB_URI || env.MONGO_URI || null;
  if (fullUri) return fullUri;

  // Otherwise, assemble from parts
  const user = env.MONGO_USER || env.MONGODB_USER || env.DB_USER || "";
  const password = env.MONGO_PASSWORD || env.MONGODB_PASSWORD || env.DB_PASSWORD || "";
  const hosts = env.MONGO_HOSTS || env.MONGODB_HOSTS || env.DB_HOSTS || env.MONGO_HOST || env.DB_HOST || "localhost:27017";
  const authSource = env.MONGO_AUTH_SOURCE || env.MONGODB_AUTH_SOURCE || env.DB_AUTHSOURCE || "admin";
  const dbName = env.MONGO_DB || env.MONGODB_DB || env.DB_NAME || "admin";
  const options = env.MONGO_OPTIONS || env.MONGODB_OPTIONS || ""; // e.g. replicaSet=rs0&tls=true

  const userinfo = user ? (password ? `${user}:${encodeURIComponent(password)}@` : `${user}@`) : "";
  const optQuery = options ? (options.startsWith("?") ? options : `?${options}`) : `?authSource=${authSource}`;
  return `mongodb://${userinfo}${hosts}/${dbName}${optQuery}`;
}

function safeRedact(uri) {
  try {
    return uri.replace(/:\\S+@/, ":***@");
  } catch { return uri; }
}

(function main() {
  print("=== MongoDB Analysis ===");
  print("Script dir:", SCRIPT_DIR);
  print("Project root:", PROJECT_ROOT);
  print(".env path:", ENV_PATH);

  const env = loadDotEnv(ENV_PATH);
  const uri = buildMongoUri(env);
  print("Connecting with URI:", safeRedact(uri));

  // Establish connection
  const conn = new Mongo(uri);

  // Select rbl DB explicitly
  const targetDbName = "rbl";
  const rblDb = conn.getDB(targetDbName);

  // Verify connection
  const ping = rblDb.runCommand({ ping: 1 });
  if (ping.ok !== 1) {
    throw new Error("Ping failed for DB 'rbl': " + tojson(ping));
  }
  // Make authenticated DB the global db so loaded scripts use it
  try { globalThis.db = rblDb; } catch {}
  print("Connected. Active DB:", rblDb.getName());

  // Helper: list collections for rbl
  function listCollections() {
    try {
      const names = rblDb.getCollectionNames();
      print("Collections in 'rbl' (", names.length, "):");
      names.forEach(n => print(" -", n));
    } catch (e) {
      print("⚠️  Unable to list collections via getCollectionNames(), using runCommand fallback.");
      const res = rblDb.runCommand({ listCollections: 1, nameOnly: true });
      if (res && res.ok === 1) {
        (res.cursor && res.cursor.firstBatch || []).forEach(c => print(" -", c.name));
      } else {
        throw new Error("listCollections failed: " + tojson(res));
      }
    }
  }

  // Flags (support both CLI args and environment variables)
  const argv = (typeof process !== 'undefined' && process.argv) ? process.argv.slice(2) : [];
  const envMode = (typeof process !== 'undefined' && process.env && process.env.ANALYSIS_MODE) ? String(process.env.ANALYSIS_MODE).toLowerCase() : '';
  const envRun = (typeof process !== 'undefined' && process.env && process.env.RUN_SCRIPT) ? String(process.env.RUN_SCRIPT) : '';
  const flagAll = argv.includes('--all') || envMode === 'all';
  const flagTikTok = argv.includes('--tiktok') || envMode === 'tiktok';
  const flagYouTube = argv.includes('--youtube') || envMode === 'youtube';
  const runIdx = argv.indexOf('--run');
  const runArg = runIdx >= 0 ? argv[runIdx + 1] : (envRun || null);

  // Script lists
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

  function ensureOutputRedirection() {
    const OUTPUT_DIR = SCRIPT_DIR + "/output";
    const HARDCODED_OUTPUT_BASE = "/opt/airflow/mongodb_scripts/output";
    try { if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true }); } catch {}
    // Helpers for inline CSV generation
    function getNestedValue(obj, path) {
      const keys = String(path).split('.');
      return keys.reduce((o, k) => (o && o[k] !== undefined ? o[k] : ''), obj);
    }
    function jsonToCSV(jsonData, headers) {
      let csv = headers.join(',') + '\n';
      jsonData.forEach(item => {
        const row = headers.map(h => {
          const v = getNestedValue(item, h);
          if (v === null || v === undefined) return '';
          if (typeof v === 'object') return '"' + JSON.stringify(v).replace(/"/g, '""') + '"';
          const s = String(v);
          return (s.includes(',') || s.includes('"') || s.includes('\n')) ? '"' + s.replace(/"/g, '""') + '"' : s;
        }).join(',');
        csv += row + '\n';
      });
      return csv;
    }
    function writeCsvBesideJson(jsonPath, jsonContent) {
      try {
        const csvPath = jsonPath.replace(/\.json$/i, '.csv');
        let data = jsonContent;
        if (typeof data === 'string') data = JSON.parse(data);
        if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object') {
          const headers = Object.keys(data[0]);
          const csv = jsonToCSV(data, headers);
          fs.writeFileSync(csvPath, csv);
        } else if (!Array.isArray(data) && typeof data === 'object') {
          const headers = Object.keys(data);
          const csv = jsonToCSV([data], headers);
          fs.writeFileSync(csvPath, csv);
        }
      } catch (e) { /* best-effort; ignore csv errors */ }
    }
    try {
      const originalWriteFileSync = fs.writeFileSync;
      fs.writeFileSync = function(path, data, options) {
        if (typeof path === 'string') {
          let rewrittenPath = path.replace(HARDCODED_OUTPUT_BASE, OUTPUT_DIR);
          const lastSlash = rewrittenPath.lastIndexOf("/");
          if (lastSlash > 0) {
            const dir = rewrittenPath.slice(0, lastSlash);
            if (!fs.existsSync(dir)) { try { fs.mkdirSync(dir, { recursive: true }); } catch {} }
          }
          const res = originalWriteFileSync.call(fs, rewrittenPath, data, options);
          if (/\.json$/i.test(rewrittenPath)) {
            writeCsvBesideJson(rewrittenPath, data);
          }
          return res;
        }
        return originalWriteFileSync.apply(fs, arguments);
      };
    } catch {}
  }

  function runScript(relPath) {
    const fullPath = relPath.startsWith('/') ? relPath : (SCRIPT_DIR + '/' + relPath);
    print("\n--- Running:", fullPath, "---");
    const start = new Date();
    const maxRetries = (typeof process !== 'undefined' && process.env && process.env.ANALYSIS_MAX_RETRIES) ? parseInt(process.env.ANALYSIS_MAX_RETRIES, 10) : 5;
    const baseBackoff = (typeof process !== 'undefined' && process.env && process.env.ANALYSIS_BACKOFF_MS) ? parseInt(process.env.ANALYSIS_BACKOFF_MS, 10) : 2000;
    let attempt = 0;
    while (true) {
      try {
        load(fullPath);
        print("✅ Completed:", fullPath, `(${(((new Date()) - start)/1000).toFixed(2)}s)`);
        return true;
      } catch (err) {
        const msg = String(err && err.message || err);
        const is429 = msg.includes('TooManyRequests') || msg.includes('Error=16500') || msg.includes('Request rate is large');
        if (is429 && attempt < maxRetries) {
          const delay = baseBackoff * Math.pow(2, attempt);
          print(`⏳ Throttled (429). Retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries})...`);
          try { sleep(delay); } catch {}
          attempt++;
          continue;
        }
        print("❌ Error in", fullPath, ":", msg);
        return false;
      }
    }
  }

  function findScriptByName(name) {
    if (!name) return null;
    if (name.includes('/') || name.endsWith('.js')) return name;
    const candidates = TIKTOK_SCRIPTS.concat(YOUTUBE_SCRIPTS).filter(p => p.endsWith('/' + name) || p.endsWith('/' + name + '.js') || p === name);
    return candidates.length ? candidates[0] : null;
  }

  // Decide what to run
  if (flagAll || flagTikTok || flagYouTube || runArg) {
    ensureOutputRedirection();
    let list = [];
    if (flagAll) list = TIKTOK_SCRIPTS.concat(YOUTUBE_SCRIPTS);
    if (flagTikTok) list = list.concat(TIKTOK_SCRIPTS);
    if (flagYouTube) list = list.concat(YOUTUBE_SCRIPTS);
    if (runArg) { const resolved = findScriptByName(runArg); if (resolved) list.push(resolved); else print("❌ Could not find script for --run:", runArg); }
    const seen = {}; const finalList = list.filter(p => (seen[p] ? false : (seen[p] = true)));
    let ok = 0; finalList.forEach(p => { if (runScript(p)) ok++; });
    print(`\n=== Completed ${ok}/${finalList.length} scripts ===`);
  } else {
    // Default behavior: just list collections
    listCollections();
    print("\nProvide mode via env or flags:");
    print("  - Env: ANALYSIS_MODE=all|tiktok|youtube, RUN_SCRIPT=<nameOrPath>");
    print("  - Flags (may be blocked by mongosh): --run <script>, --tiktok, --youtube, --all");
  }
})();


