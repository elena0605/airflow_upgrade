#!/bin/bash

# Generate HTML reports from R Markdown files
# Usage: ./generate_reports.sh

set -e

echo "=== Generating Analysis Reports ==="

# Check if R is available
if ! command -v R &> /dev/null; then
    echo "❌ Error: R is not installed or not in PATH"
    echo "Please install R and R packages: rmarkdown, dplyr, ggplot2, etc."
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "tiktok_analysis_report.Rmd" ]; then
    echo "❌ Error: R Markdown files not found"
    echo "Please run this script from the mongodb_scripts/reports directory"
    exit 1
fi

# Check if output data exists
if [ ! -d "../output" ]; then
    echo "❌ Error: Output data directory not found"
    echo "Please run the MongoDB analysis first to generate CSV data"
    exit 1
fi

# Generate reports using R
echo "📊 Generating reports..."
Rscript generate_reports.R

echo ""
echo "✅ Report generation complete!"
echo "📁 HTML reports are available in: html_reports/"
echo ""
echo "To view the reports:"
echo "  - TikTok: open html_reports/tiktok_analysis_report.html"
echo "  - YouTube: open html_reports/youtube_analysis_report.html"
