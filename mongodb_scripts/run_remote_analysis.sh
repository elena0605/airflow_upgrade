#!/bin/bash

# Script to run all MongoDB analysis against remote database
# Usage: ./run_remote_analysis.sh "your-connection-string"

set -e  # Exit on any error

# Check if connection string provided
if [ $# -eq 0 ]; then
    echo "❌ Error: Please provide your MongoDB connection string"
    echo "Usage: $0 \"mongodb://user:pass@host:port/database\""
    echo "Example: $0 \"mongodb://rbl@your-host:27017/your-db?authSource=admin\""
    exit 1
fi

CONNECTION_STRING="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 Starting MongoDB Analysis Pipeline"
echo "📅 Timestamp: $(date)"
echo "🔗 Connecting to: ${CONNECTION_STRING:0:20}..."
echo "📁 Script directory: $SCRIPT_DIR"
echo ""

# Check if mongosh is available
if ! command -v mongosh &> /dev/null; then
    echo "❌ mongosh not found. Installing via Homebrew..."
    brew update
    brew tap mongodb/brew
    brew install mongosh
fi

# Run the master analysis script (pass URI via env so script can connect first)
echo "📊 Running all analysis scripts..."
MONGODB_URI="$CONNECTION_STRING" mongosh --file "$SCRIPT_DIR/run_all_analysis.js"

echo ""
echo "✅ Analysis pipeline completed!"
echo "📁 Check output files in: $SCRIPT_DIR/output/"
