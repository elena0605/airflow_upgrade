var results = [];

db.youtube_channel_stats.aggregate([
    // Stage 1: Create a document with validation results for each field
    {
      $project: {
        _id: 1,
        title: 1,
        validations: {
          channel_id: {
            value: "$channel_id",
            isEmpty: {
              $or: [
                { $eq: ["$channel_id", null] },
                { $eq: ["$channel_id", ""] },
                { $eq: [{ $type: "$channel_id" }, "missing"] },
                { $eq: [{ $trim: { input: "$channel_id" } }, ""] }
              ]
            }
          },
          title: {
            value: "$title",
            isEmpty: {
              $or: [
                { $eq: ["$title", null] },
                { $eq: ["$title", ""] },
                { $eq: [{ $type: "$title" }, "missing"] },
                { $eq: [{ $trim: { input: "$title" } }, ""] }
              ]
            }
          },
          view_count: {
            value: "$view_count",
            isEmpty: {
              $or: [
                { $eq: ["$view_count", null] },
                { $eq: ["$view_count", ""] },
                { $eq: [{ $type: "$view_count" }, "missing"] },
                { $eq: [{ $trim: { input: "$view_count" } }, ""] }
              ]
            }
          },
          subscriber_count: {
            value: "$subscriber_count",
            isEmpty: {
              $or: [
                { $eq: ["$subscriber_count", null] },
                { $eq: ["$subscriber_count", ""] },
                { $eq: [{ $type: "$subscriber_count" }, "missing"] },
                { $eq: [{ $trim: { input: "$subscriber_count" } }, ""] }
              ]
            }
          },
          video_count: {
            value: "$video_count",
            isEmpty: {
              $or: [
                { $eq: ["$video_count", null] },
                { $eq: ["$video_count", ""] },
                { $eq: [{ $type: "$video_count" }, "missing"] },
                { $eq: [{ $trim: { input: "$video_count" } }, ""] }
              ]
            }
          },
          description: {
            value: "$description",
            isEmpty: {
              $or: [
                { $eq: ["$description", null] },
                { $eq: ["$description", ""] },
                { $eq: ["$description", "Unknown"] },  // Check for "Unknown" value
                { $eq: [{ $type: "$description" }, "missing"] },
                { $eq: [{ $trim: { input: { $ifNull: ["$description", ""] } } }, ""] }
              ]
            }
          },
          keywords: {
            value: "$keywords",
            isEmpty: {
              $or: [
                { $eq: ["$keywords", null] },
                { $eq: ["$keywords", []] },
                { $eq: ["$keywords", ""] },
                { $eq: [{ $type: "$keywords" }, "missing"] }
              ]
            }
          },
          country: {
            value: "$country",
            isEmpty: {
              $or: [
                { $eq: ["$country", null] },
                { $eq: ["$country", ""] },
                { $eq: [{ $type: "$country" }, "missing"] },
                { $eq: [{ $trim: { input: { $ifNull: ["$country", ""] } } }, ""] }
              ]
            }
          },
          topic_categories: {
            value: "$topic_categories",
            isEmpty: {
              $or: [
                { $eq: ["$topic_categories", null] },
                { $eq: ["$topic_categories", []] },
                { $eq: [{ $type: "$topic_categories" }, "missing"] }
              ]
            }
          }
        }
      }
    },
    
    // Stage 2: Find documents with empty fields
    {
      $match: {
        $or: [
          { "validations.channel_id.isEmpty": true },
          { "validations.title.isEmpty": true },
          { "validations.view_count.isEmpty": true },
          { "validations.subscriber_count.isEmpty": true },
          { "validations.video_count.isEmpty": true },
          { "validations.description.isEmpty": true },
          { "validations.keywords.isEmpty": true },
          { "validations.country.isEmpty": true },
          { "validations.topic_categories.isEmpty": true }
        ]
      }
    },
    
    // Stage 3: Format the output
    {
      $project: {
        _id: 0,
        "Channel": "$title",
        "Empty Fields": {
          $objectToArray: {
            channel_id: "$validations.channel_id.isEmpty",
            title: "$validations.title.isEmpty",
            view_count: "$validations.view_count.isEmpty",
            subscriber_count: "$validations.subscriber_count.isEmpty",
            video_count: "$validations.video_count.isEmpty",
            description: "$validations.description.isEmpty",
            keywords: "$validations.keywords.isEmpty",
            country: "$validations.country.isEmpty",
            topic_categories: "$validations.topic_categories.isEmpty"
          }
        }
      }
    },
    
    // Stage 4: Unwind the array of empty fields
    {
      $unwind: "$Empty Fields"
    },
    
    // Stage 5: Keep only the empty fields
    {
      $match: {
        "Empty Fields.v": true
      }
    },
    
    // Stage 6: Group by channel
    {
      $group: {
        _id: "$Channel",
        "Empty Fields": { $push: "$Empty Fields.k" }
      }
    },
    
    // Stage 7: Final format
    {
      $project: {
        _id: 0,
        "Channel": "$_id",
        "Empty Fields": 1
      }
    },
    
    // Stage 8: Sort by channel name
    {
      $sort: { "Channel": 1 }
    }
    ]).forEach(function(doc) {
    results.push(doc);
});

// Save to file
var outputPath = "/opt/airflow/mongodb_scripts/output/youtube/channel_empty_fields.json";
fs.writeFileSync(outputPath, JSON.stringify(results, null, 2));
print("Results saved to: " + outputPath);