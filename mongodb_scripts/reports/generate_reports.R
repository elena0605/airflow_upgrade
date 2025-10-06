#!/usr/bin/env Rscript

# Generate HTML reports from R Markdown files
# Usage: Rscript generate_reports.R

library(rmarkdown)
library(here)

# Set working directory to the reports folder
setwd(here::here("mongodb_scripts", "reports"))

# Function to generate report
generate_report <- function(rmd_file, output_dir = "html_reports") {
  if (!file.exists(rmd_file)) {
    cat("Error: R Markdown file not found:", rmd_file, "\n")
    return(FALSE)
  }
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Generate HTML report
  output_file <- file.path(output_dir, gsub("\\.Rmd$", ".html", basename(rmd_file)))
  
  cat("Generating report:", rmd_file, "->", output_file, "\n")
  
  tryCatch({
    render(
      input = rmd_file,
      output_file = output_file,
      output_format = html_document(
        toc = TRUE,
        toc_float = TRUE,
        theme = "cosmo",
        code_folding = "hide",
        code_menu = TRUE,
        df_print = "paged"
      ),
      params = list(
        data_path = "../output"
      )
    )
    cat("✅ Successfully generated:", output_file, "\n")
    return(TRUE)
  }, error = function(e) {
    cat("❌ Error generating", rmd_file, ":", e$message, "\n")
    return(FALSE)
  })
}

# Main execution
cat("=== Generating Analysis Reports ===\n")
cat("Working directory:", getwd(), "\n")

# Check if we're in the right directory
if (!file.exists("tiktok_analysis_report.Rmd")) {
  cat("Error: R Markdown files not found. Please run from mongodb_scripts/reports directory.\n")
  quit(status = 1)
}

# Generate TikTok report
cat("\n--- Generating TikTok Report ---\n")
tiktok_success <- generate_report("tiktok_analysis_report.Rmd")

# Generate YouTube report
cat("\n--- Generating YouTube Report ---\n")
youtube_success <- generate_report("youtube_analysis_report.Rmd")

# Summary
cat("\n=== Report Generation Summary ===\n")
cat("TikTok Report:", if(tiktok_success) "✅ Success" else "❌ Failed", "\n")
cat("YouTube Report:", if(youtube_success) "✅ Success" else "❌ Failed", "\n")

if (tiktok_success && youtube_success) {
  cat("\n🎉 All reports generated successfully!\n")
  cat("HTML files are available in: html_reports/\n")
} else {
  cat("\n⚠️  Some reports failed to generate. Check the error messages above.\n")
  quit(status = 1)
}
