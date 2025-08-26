
const fs = require('fs');
const path = require('path');

const csvPath = path.join(__dirname, "output", "youtube", "Top 200 YouTube Influencers.csv");
const biosPath = path.join(__dirname, "output", "youtube", "user_descriptions.json");
const outputPath = path.join(__dirname, "output", "youtube", "Top 200 YouTube Influencers with Description.csv");

// Load bios
const userBios = {};
const bios = JSON.parse(fs.readFileSync(biosPath, "utf-8"));
bios.forEach(entry => {
  userBios[entry.username.toLowerCase()] = entry.description;
});

// Read and parse CSV
const csvContent = fs.readFileSync(csvPath, "utf-8");
const csvLines = csvContent.split("\n").filter(line => line.trim() !== "");
const header = parseCSVLine(csvLines[0]);

// Determine index of InfluencerStandardized
const influencerIndex = header.findIndex(h => h.trim().toLowerCase() === "influencerstandardized");
if (influencerIndex === -1) {
  throw new Error("InfluencerStandardized column not found in CSV.");
}

const newHeader = [...header, "description"];
const outputLines = [formatCSVLine(newHeader)];

for (let i = 1; i < csvLines.length; i++) {
  const line = csvLines[i];
  const columns = parseCSVLine(line);
  if (columns.length < header.length) continue;

  const rawUsername = columns[influencerIndex] || "";
  const username = rawUsername.replace(/^@/, "").toLowerCase().trim();

  const description = userBios[username] || "N/A";
  const newRow = [...columns, description];

  outputLines.push(formatCSVLine(newRow));
}

fs.writeFileSync(outputPath, outputLines.join("\n"));
console.log(`Successfully wrote ${outputLines.length - 1} rows to ${outputPath}`);

// --- CSV helpers
function parseCSVLine(line) {
  const result = [];
  let inQuotes = false;
  let current = "";

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  result.push(current);
  return result;
}

function formatCSVLine(values) {
  return values.map(value => {
    if (typeof value === "string" && (value.includes(",") || value.includes('"') || value.includes("\n"))) {
      return `"${value.replace(/"/g, '""')}"`;
    }
    return `"${value}"`;
  }).join(",");
}
