import pandas as pd
import os
import sys
from pathlib import Path

# Get the directory of the script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.join(SCRIPT_DIR, "csv_files")

# File paths
PERSON_NODES_CSV = os.path.join(CSV_DIR, "person_nodes.csv")
TOP200TT_CSV = os.path.join(CSV_DIR, "Top200TT.csv")
TOP200YT_CSV = os.path.join(CSV_DIR, "Top200YT .csv")
INFLUENCER_ACCOUNTS_CSV = os.path.join(CSV_DIR, "influencer_accounts.csv")
FOLLOWS_CSV = os.path.join(CSV_DIR, "follows.csv")
CYPHER_SCRIPT = os.path.join(SCRIPT_DIR, "create_follows_cypher.cypher")

def normalize_account_value(value):
    """Normalize account value for comparison."""
    if pd.isna(value) or value == "" or str(value).strip().upper() == "NA":
        return None
    # Remove @ symbol if present, lowercase, strip whitespace
    val = str(value).strip().lower()
    if val.startswith("@"):
        val = val[1:]
    return val

def build_influencer_list_mapping():
    """Build a mapping from InfluencerList values to Handle values from Top200TT and Top200YT."""
    mapping = {}
    
    print("Building influencer list mapping from Top200TT.csv...")
    df_tt = pd.read_csv(TOP200TT_CSV, sep=';', na_values=[], keep_default_na=False)
    
    for idx, row in df_tt.iterrows():
        handle = str(row.get("Handle", "")).strip()
        if handle == "" or handle == "NA":
            continue
        
        influencer_list = str(row.get("InfluencerList", "")).strip()
        if influencer_list == "" or influencer_list == "NA":
            continue
        
        # Split the InfluencerList by comma and process each value
        for item in influencer_list.split(","):
            item = normalize_account_value(item)
            if item:
                # Store mapping: normalized item -> handle
                if item not in mapping:
                    mapping[item] = []
                if handle not in mapping[item]:
                    mapping[item].append(handle)
    
    print(f"Found {len(mapping)} unique TikTok mappings")
    
    print("Building influencer list mapping from Top200YT.csv...")
    df_yt = pd.read_csv(TOP200YT_CSV, sep=';', na_values=[], keep_default_na=False)
    
    for idx, row in df_yt.iterrows():
        handle = str(row.get("Handle", "")).strip()
        if handle == "" or handle == "NA":
            continue
        
        influencer_list = str(row.get("InfluencerList", "")).strip()
        if influencer_list == "" or influencer_list == "NA":
            continue
        
        # Split the InfluencerList by comma and process each value
        for item in influencer_list.split(","):
            item = normalize_account_value(item)
            if item:
                # Store mapping: normalized item -> handle
                if item not in mapping:
                    mapping[item] = []
                if handle not in mapping[item]:
                    mapping[item].append(handle)
    
    print(f"Found {len(mapping)} unique total mappings (TikTok + YouTube)")
    return mapping

def find_handle_for_account(account_value, mapping):
    """Find handle(s) for a given account value using the mapping."""
    if not account_value:
        return []
    
    normalized = normalize_account_value(account_value)
    if not normalized:
        return []
    
    # Try exact match first
    if normalized in mapping:
        return mapping[normalized]
    
    # Try partial match (if normalized value is a substring of any key)
    matches = []
    for key, handles in mapping.items():
        if normalized in key or key in normalized:
            matches.extend(handles)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_matches = []
    for handle in matches:
        if handle not in seen:
            seen.add(handle)
            unique_matches.append(handle)
    
    return unique_matches

def create_follows_csv():
    """Create follows.csv with UserID, tiktok_handle, and youtube_handle columns."""
    print(f"Reading {PERSON_NODES_CSV}...")
    df_person = pd.read_csv(PERSON_NODES_CSV, na_values=[], keep_default_na=False)
    
    print("Building influencer list mapping...")
    mapping = build_influencer_list_mapping()
    
    # Track statistics
    stats = {
        'total_persons': len(df_person),
        'persons_with_matches': 0,
        'tiktok_matches': 0,
        'youtube_matches': 0,
        'total_tiktok_accounts_checked': 0,
        'total_youtube_accounts_checked': 0,
        'tiktok_accounts_matched': 0,
        'youtube_accounts_matched': 0
    }
    
    # Columns to check
    tiktok_cols = ["tiktok_account1", "tiktok_account2", "tiktok_account3"]
    youtube_cols = ["youtube_account1", "youtube_account2", "youtube_account3"]
    
    # Prepare results
    results = []
    
    print("Processing person accounts and matching with handles...")
    for idx, row in df_person.iterrows():
        user_id = row["UserID"]
        
        # Collect all TikTok handles for this person
        tiktok_handles = set()
        for col in tiktok_cols:
            account_value = row.get(col, None)
            stats['total_tiktok_accounts_checked'] += 1
            if account_value:
                handles = find_handle_for_account(account_value, mapping)
                if handles:
                    stats['tiktok_accounts_matched'] += 1
                    tiktok_handles.update(handles)
        
        # Collect all YouTube handles for this person
        youtube_handles = set()
        for col in youtube_cols:
            account_value = row.get(col, None)
            stats['total_youtube_accounts_checked'] += 1
            if account_value:
                handles = find_handle_for_account(account_value, mapping)
                if handles:
                    stats['youtube_accounts_matched'] += 1
                    youtube_handles.update(handles)
        
        # Create a row for each unique combination of UserID and handles
        if tiktok_handles or youtube_handles:
            stats['persons_with_matches'] += 1
            # If we have multiple handles, we'll create multiple rows
            # But for now, let's combine all handles into comma-separated values
            tiktok_handle_str = ",".join(sorted(tiktok_handles)) if tiktok_handles else ""
            youtube_handle_str = ",".join(sorted(youtube_handles)) if youtube_handles else ""
            
            results.append({
                'UserID': user_id,
                'tiktok_handle': tiktok_handle_str,
                'youtube_handle': youtube_handle_str
            })
            
            if tiktok_handles:
                stats['tiktok_matches'] += 1
            if youtube_handles:
                stats['youtube_matches'] += 1
    
    print("\n=== Step 1-2 Statistics ===")
    print(f"Total persons processed: {stats['total_persons']}")
    print(f"Persons with matches: {stats['persons_with_matches']}")
    print(f"Total TikTok accounts checked: {stats['total_tiktok_accounts_checked']}")
    print(f"TikTok accounts matched: {stats['tiktok_accounts_matched']}")
    print(f"Total YouTube accounts checked: {stats['total_youtube_accounts_checked']}")
    print(f"YouTube accounts matched: {stats['youtube_accounts_matched']}")
    print(f"Persons with TikTok matches: {stats['tiktok_matches']}")
    print(f"Persons with YouTube matches: {stats['youtube_matches']}")
    
    # Create DataFrame and save
    df_follows = pd.DataFrame(results)
    df_follows.to_csv(FOLLOWS_CSV, index=False)
    print(f"\nCreated {FOLLOWS_CSV} with {len(df_follows)} records")
    
    return df_follows, stats

def normalize_name(name):
    """Normalize influencer name for comparison."""
    if pd.isna(name) or name == "":
        return None
    # Lowercase, remove all spaces, remove special characters like ·, -, etc.
    normalized = str(name).strip().lower()
    normalized = normalized.replace("·", "").replace("-", "").replace("_", "").replace(" ", "")
    return normalized

def match_with_influencer_accounts(df_follows):
    """Match follows.csv handles with influencer_accounts.csv to add influencer column."""
    print(f"\nReading {INFLUENCER_ACCOUNTS_CSV}...")
    df_influencer = pd.read_csv(INFLUENCER_ACCOUNTS_CSV, sep=';', na_values=[], keep_default_na=False)
    
    # Build handle -> Name mapping from Top200TT and Top200YT
    print("Building Handle -> Name mapping from Top200TT/Top200YT...")
    handle_to_name = {}
    df_tt = pd.read_csv(TOP200TT_CSV, sep=';', na_values=[], keep_default_na=False)
    for idx, row in df_tt.iterrows():
        handle = normalize_account_value(row.get("Handle", None))
        name = str(row.get("Name", "")).strip()
        if handle and name:
            handle_to_name[handle] = name
    
    df_yt = pd.read_csv(TOP200YT_CSV, sep=';', na_values=[], keep_default_na=False)
    for idx, row in df_yt.iterrows():
        handle = normalize_account_value(row.get("Handle", None))
        name = str(row.get("Name", "")).strip()
        if handle and name:
            handle_to_name[handle] = name
    
    # Create a mapping from handles to influencer names
    handle_to_influencer = {}
    # Also create a mapping from normalized influencer names to influencer names
    name_to_influencer = {}
    
    for idx, row in df_influencer.iterrows():
        influencer = str(row.get("influencer", "")).strip()
        tiktok_acc = normalize_account_value(row.get("tiktok_account", None))
        youtube_acc = normalize_account_value(row.get("youtube_account", None))
        
        # Add to name mapping (normalized)
        norm_name = normalize_name(influencer)
        if norm_name:
            if norm_name not in name_to_influencer:
                name_to_influencer[norm_name] = []
            if influencer not in name_to_influencer[norm_name]:
                name_to_influencer[norm_name].append(influencer)
        
        if tiktok_acc:
            if tiktok_acc not in handle_to_influencer:
                handle_to_influencer[tiktok_acc] = []
            if influencer not in handle_to_influencer[tiktok_acc]:
                handle_to_influencer[tiktok_acc].append(influencer)
        
        if youtube_acc:
            if youtube_acc not in handle_to_influencer:
                handle_to_influencer[youtube_acc] = []
            if influencer not in handle_to_influencer[youtube_acc]:
                handle_to_influencer[youtube_acc].append(influencer)
    
    print(f"Built mapping from {len(handle_to_influencer)} handles to influencers")
    print(f"Built mapping from {len(name_to_influencer)} normalized names to influencers")
    
    # Track statistics
    stats = {
        'total_follows_records': len(df_follows),
        'records_with_influencer_match': 0,
        'tiktok_handle_matches': 0,
        'youtube_handle_matches': 0,
        'total_tiktok_handles_checked': 0,
        'total_youtube_handles_checked': 0,
        'tiktok_handles_matched': 0,
        'youtube_handles_matched': 0
    }
    
    # Expand follows records: one row per handle-influencer combination
    expanded_results = []
    
    print("Matching handles with influencer accounts...")
    for idx, row in df_follows.iterrows():
        user_id = row["UserID"]
        tiktok_handles_str = str(row.get("tiktok_handle", "")).strip()
        youtube_handles_str = str(row.get("youtube_handle", "")).strip()
        
        # Parse comma-separated handles
        tiktok_handles = [h.strip() for h in tiktok_handles_str.split(",") if h.strip()] if tiktok_handles_str else []
        youtube_handles = [h.strip() for h in youtube_handles_str.split(",") if h.strip()] if youtube_handles_str else []
        
        # Find influencers for TikTok handles
        influencers_found = set()
        for handle in tiktok_handles:
            stats['total_tiktok_handles_checked'] += 1
            normalized_handle = normalize_account_value(handle)
            
            # First try matching by handle
            if normalized_handle and normalized_handle in handle_to_influencer:
                stats['tiktok_handles_matched'] += 1
                influencers_found.update(handle_to_influencer[normalized_handle])
            # If handle match fails, try matching by name from Top200TT/Top200YT
            elif normalized_handle and normalized_handle in handle_to_name:
                top200_name = handle_to_name[normalized_handle]
                norm_top200_name = normalize_name(top200_name)
                if norm_top200_name and norm_top200_name in name_to_influencer:
                    stats['tiktok_handles_matched'] += 1
                    influencers_found.update(name_to_influencer[norm_top200_name])
        
        # Find influencers for YouTube handles
        for handle in youtube_handles:
            stats['total_youtube_handles_checked'] += 1
            normalized_handle = normalize_account_value(handle)
            
            # First try matching by handle
            if normalized_handle and normalized_handle in handle_to_influencer:
                stats['youtube_handles_matched'] += 1
                influencers_found.update(handle_to_influencer[normalized_handle])
            # If handle match fails, try matching by name from Top200TT/Top200YT
            elif normalized_handle and normalized_handle in handle_to_name:
                top200_name = handle_to_name[normalized_handle]
                norm_top200_name = normalize_name(top200_name)
                if norm_top200_name and norm_top200_name in name_to_influencer:
                    stats['youtube_handles_matched'] += 1
                    influencers_found.update(name_to_influencer[norm_top200_name])
        
        if influencers_found:
            stats['records_with_influencer_match'] += 1
            if influencers_found:
                stats['tiktok_handle_matches'] += 1
            if influencers_found:
                stats['youtube_handle_matches'] += 1
            
            # Create one row per influencer
            for influencer in influencers_found:
                expanded_results.append({
                    'UserID': user_id,
                    'tiktok_handle': tiktok_handles_str,
                    'youtube_handle': youtube_handles_str,
                    'Influencer': influencer
                })
        else:
            # Keep record even without influencer match
            expanded_results.append({
                'UserID': user_id,
                'tiktok_handle': tiktok_handles_str,
                'youtube_handle': youtube_handles_str,
                'Influencer': ""
            })
    
    print("\n=== Step 3 Statistics ===")
    print(f"Total follows records processed: {stats['total_follows_records']}")
    print(f"Records with influencer match: {stats['records_with_influencer_match']}")
    print(f"Total TikTok handles checked: {stats['total_tiktok_handles_checked']}")
    print(f"TikTok handles matched: {stats['tiktok_handles_matched']}")
    print(f"Total YouTube handles checked: {stats['total_youtube_handles_checked']}")
    print(f"YouTube handles matched: {stats['youtube_handles_matched']}")
    
    # Update follows.csv with influencer column
    df_follows_updated = pd.DataFrame(expanded_results)
    df_follows_updated.to_csv(FOLLOWS_CSV, index=False)
    print(f"\nUpdated {FOLLOWS_CSV} with {len(df_follows_updated)} records (including influencer column)")
    
    return df_follows_updated, stats

def create_cypher_script(df_follows):
    """Create Cypher script to create FOLLOWS relationships."""
    print(f"\nCreating Cypher script: {CYPHER_SCRIPT}")
    
    # Filter to only records with influencer matches
    df_with_influencers = df_follows[df_follows['Influencer'].notna() & (df_follows['Influencer'].str.strip() != "")]
    
    print(f"Found {len(df_with_influencers)} records with influencer matches")
    
    cypher_lines = [
        "// Cypher script to create FOLLOWS relationships between Person and Influencer nodes",
        "// Generated automatically from follows.csv",
        "",
        f"// Total relationships to create: {len(df_with_influencers)}",
        "",
        "// Create FOLLOWS relationships",
    ]
    
    # Use batch processing for efficiency
    batch_size = 1000
    batch_count = 0
    
    for idx, row in df_with_influencers.iterrows():
        user_id = row["UserID"]
        influencer = str(row["Influencer"]).strip()
        
        if batch_count == 0:
            cypher_lines.append("")
            cypher_lines.append("// Batch processing - creating relationships...")
            cypher_lines.append("UNWIND [")
        
        # Escape quotes in influencer name
        influencer_escaped = influencer.replace("'", "\\'").replace('"', '\\"')
        
        cypher_lines.append(f"  {{user_id: {user_id}, influencer: '{influencer_escaped}'}},")
        
        batch_count += 1
        
        if batch_count >= batch_size:
            # Close current batch and create relationships
            cypher_lines[-1] = cypher_lines[-1].rstrip(',')  # Remove trailing comma
            cypher_lines.append("] AS batch")
            cypher_lines.append("MATCH (p:Person {UserID: batch.user_id})")
            cypher_lines.append("MATCH (i:Influencer {name: batch.influencer})")
            cypher_lines.append("MERGE (p)-[:FOLLOWS]->(i)")
            cypher_lines.append("")
            batch_count = 0
    
    # Close last batch if any
    if batch_count > 0:
        cypher_lines[-1] = cypher_lines[-1].rstrip(',')  # Remove trailing comma
        cypher_lines.append("] AS batch")
        cypher_lines.append("MATCH (p:Person {UserID: batch.user_id})")
        cypher_lines.append("MATCH (i:Influencer {name: batch.influencer})")
        cypher_lines.append("MERGE (p)-[:FOLLOWS]->(i)")
        cypher_lines.append("")
    
    # Add final statistics query
    cypher_lines.extend([
        "// Verify relationships created",
        "MATCH ()-[r:FOLLOWS]->()",
        "RETURN count(r) AS total_follows_relationships;",
        ""
    ])
    
    # Write to file
    with open(CYPHER_SCRIPT, 'w', encoding='utf-8') as f:
        f.write('\n'.join(cypher_lines))
    
    print(f"Created Cypher script: {CYPHER_SCRIPT}")
    print(f"Total FOLLOWS relationships to create: {len(df_with_influencers)}")

def main():
    """Main function."""
    print("=" * 60)
    print("Creating FOLLOWS relationships - Processing Pipeline")
    print("=" * 60)
    
    try:
        # Step 1-2: Create follows.csv with handles
        df_follows, stats1 = create_follows_csv()
        
        # Step 3: Match with influencer_accounts.csv
        df_follows_updated, stats2 = match_with_influencer_accounts(df_follows)
        
        # Step 4: Track mapping success (already done in stats)
        print("\n=== Overall Mapping Success Summary ===")
        print(f"Step 1-2: Found handles for {stats1['persons_with_matches']} out of {stats1['total_persons']} persons")
        print(f"Step 3: Matched influencers for {stats2['records_with_influencer_match']} out of {stats2['total_follows_records']} follows records")
        
        # Calculate final success rate
        total_records_with_influencers = len(df_follows_updated[df_follows_updated['Influencer'].notna() & (df_follows_updated['Influencer'].str.strip() != "")])
        print(f"Final: {total_records_with_influencers} Person-Influencer relationships ready to create")
        
        # Step 5: Create Cypher script
        create_cypher_script(df_follows_updated)
        
        print("\n" + "=" * 60)
        print("Processing complete!")
        print("=" * 60)
        
    except FileNotFoundError as e:
        print(f"File error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

