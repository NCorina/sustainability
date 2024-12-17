import pandas as pd


import os
print(os.getcwd())


from chardet.universaldetector import UniversalDetector

# Function to detect the encoding of a file
def detect_encoding(file_path):
    detector = UniversalDetector()
    with open(file_path, "rb") as file:
        for line in file:
            detector.feed(line)
            if detector.done:
                break
        detector.close()
    return detector.result["encoding"]



# Function to load a file with error handling for parsing and encoding
def load_file_with_handling(file_path, expected_columns):
    try:
        # Step 1: Detect Encoding
        encoding = detect_encoding(file_path)
        print(f"Detected encoding for {file_path}: {encoding}")

        # Step 2: Attempt to Load File
        data = pd.read_csv(
            file_path,
            encoding=encoding,         # Use detected encoding
            on_bad_lines="skip",       # Skip problematic rows
            engine="python",           # Use Python engine for flexibility
        )
        print(f"Successfully loaded file: {file_path}")
        return data

    except UnicodeDecodeError as e:
        print(f"Encoding error for {file_path}: {e}")
        print("Trying with 'ISO-8859-1' encoding as fallback...")
        try:
            # Attempt to load with a fallback encoding
            data = pd.read_csv(
                file_path,
                encoding="ISO-8859-1",      # Fallback encoding
                on_bad_lines="skip",
                engine="python",
            )
            print(f"Successfully loaded file with fallback encoding: {file_path}")
            return data
        except Exception as e:
            print(f"Failed to load file with fallback encoding: {file_path}")
            print(e)
            return None

    except pd.errors.ParserError as e:
        print(f"Parsing error for {file_path}: {e}")
        print(f"Check for inconsistent rows in {file_path}")
        return None

# File paths
msci_file_path = "C:/Users/corin/Desktop/MSCI KLD data csv.csv"
bcorp_file_path = "C:/Users/corin/Desktop/B Corp Impact Data.csv"

# Load MSCI and B Corp files with handling
msci_data = load_file_with_handling(msci_file_path, expected_columns=156)
bcorp_data = load_file_with_handling(bcorp_file_path, expected_columns=135)

# Preview the datasets if loaded successfully
if msci_data is not None:
    print("MSCI Data Preview:")
    print(msci_data.head())
if bcorp_data is not None:
    print("B Corp Data Preview:")
    print(bcorp_data.head())

# Function to normalize text
def normalize_text(text):
    if pd.isnull(text):  # Handle missing values
        return ""
    return (
        text.lower()           # Convert to lowercase
        .strip()               # Remove leading/trailing whitespace
        .replace(",", "")      # Remove commas
        .replace(".", "")      # Remove periods
        .replace("&", "and")   # Replace ampersand with "and"
        .replace("-", "")      # Remove hyphens
    )

# Normalize the company names in both files
msci_data["NormalizedName"] = msci_data["ISSUER_NAME"].apply(normalize_text)
bcorp_data["NormalizedName"] = bcorp_data["company_name"].apply(normalize_text)
# Perform an exact match
matched_data = pd.merge(msci_data, bcorp_data, on="NormalizedName", how="inner")

# Save or display the matched data
print("Number of Matches:", matched_data.shape[0])
matched_data.to_excel("matched_data_full.xlsx", index=False)
from rapidfuzz import process, fuzz

# Function to normalize text
def normalize_text(text):
    if pd.isnull(text):  # Handle missing values
        return ""
    return (
        text.lower()           # Convert to lowercase
        .strip()               # Remove leading/trailing whitespace
        .replace(",", "")      # Remove commas
        .replace(".", "")      # Remove periods
        .replace("&", "and")   # Replace ampersand with "and"
        .replace("-", "")      # Remove hyphens
    )

# Normalize company names in both datasets
msci_data["NormalizedName"] = msci_data["ISSUER_NAME"].apply(normalize_text)
bcorp_data["NormalizedName"] = bcorp_data["company_name"].apply(normalize_text)
# Function for fuzzy matching
def fuzzy_match(msci_names, bcorp_names, threshold=85):
    matches = []
    for msci_name in msci_names:
        match = process.extractOne(msci_name, bcorp_names, scorer=fuzz.token_sort_ratio)
        if match and match[1] >= threshold:  # Match score >= threshold
            matches.append((msci_name, match[0], match[1]))  # (MSCI Name, Matched B Corp Name, Score)
    return matches

# Perform fuzzy matching
msci_names = msci_data["NormalizedName"].tolist()
bcorp_names = bcorp_data["NormalizedName"].tolist()
fuzzy_matches = fuzzy_match(msci_names, bcorp_names)

# Convert matches to a DataFrame
fuzzy_matches_df = pd.DataFrame(fuzzy_matches, columns=["MSCI_Name", "Bcorp_Name", "Match_Score"])

# Save fuzzy matches for review
fuzzy_matches_df.to_excel("fuzzy_matches.xlsx", index=False)
print("Fuzzy matches saved to 'fuzzy_matches.xlsx'")
# Filter matches with a high score
high_confidence_matches = fuzzy_matches_df[fuzzy_matches_df["Match_Score"] >= 85]

# Save high-confidence matches for review
high_confidence_matches.to_excel("high_confidence_fuzzy_matches.xlsx", index=False)
print("High-confidence fuzzy matches saved to 'high_confidence_fuzzy_matches.xlsx'")
# Merge exact matches and high-confidence fuzzy matches
combined_matches = pd.merge(msci_data, high_confidence_matches, left_on="NormalizedName", right_on="MSCI_Name", how="inner")

# Save combined matches
combined_matches.to_excel("combined_matches.xlsx", index=False)
print("Combined matches saved to 'combined_matches.xlsx'")
# Mark MSCI companies as matched or not
msci_data["IsBcorp"] = msci_data["NormalizedName"].isin(high_confidence_matches["MSCI_Name"]).astype(int)

# Save the updated MSCI dataset
msci_data.to_excel("msci_with_bcorp_status.xlsx", index=False)
print("Updated MSCI dataset saved to 'msci_with_bcorp_status.xlsx'")