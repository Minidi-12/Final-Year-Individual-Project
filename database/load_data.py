# Loads all real collected data into mhealth_lk database

import pandas as pd
import os
from sqlalchemy import create_engine, text
from openpyxl import load_workbook
from dotenv import load_dotenv
from urllib.parse import quote_plus
load_dotenv()

# ── DATABASE CONNECTION ──────────────────────────────────────────
print("Connecting to mhealth_lk database...")

# quote_plus handles special characters like @ automatically
password = quote_plus(os.getenv('DB_PASSWORD'))

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:"
    f"{password}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

# Test connection
try:
    with engine.connect() as conn:
        conn.execute(text('SELECT 1'))
    print("✅ Connected to database successfully!")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit()

# ================================================================
# STEP 1: Load 25 Districts from Your Excel File
# ================================================================
print("\n--- STEP 1: Loading Districts ---")

PROVINCE_MAP = {
    'Colombo': 'Western',      'Gampaha': 'Western',
    'Kalutara': 'Western',     'Kandy': 'Central',
    'Matale': 'Central',       'Nuwara Eliya': 'Central',
    'Galle': 'Southern',       'Matara': 'Southern',
    'Hambantota': 'Southern',  'Jaffna': 'Northern',
    'Kilinochchi': 'Northern', 'Mannar': 'Northern',
    'Vavuniya': 'Northern',    'Mullaitivu': 'Northern',
    'Batticaloa': 'Eastern',   'Ampara': 'Eastern',
    'Trincomalee': 'Eastern',  'Kurunegala': 'North Western',
    'Puttalam': 'North Western','Anuradhapura': 'North Central',
    'Polonnaruwa': 'North Central', 'Badulla': 'Uva',
    'Monaragala': 'Uva',       'Ratnapura': 'Sabaragamuwa',
    'Kegalle': 'Sabaragamuwa'
}

try:
    wb = load_workbook('data/District_Data_Extraction.xlsx',
                       read_only=True)
    ws = wb['Sheet2']
    rows = list(ws.iter_rows(values_only=True))

    districts_data = []
    for row in rows[1:]:  # skip header row
        if row[0] is None:
            continue
        name = row[0]

        # Calculate total females 10-49
        female_vals = [v for v in row[2:10]
                       if isinstance(v, (int, float))]
        f_total = int(sum(female_vals))

        districts_data.append({
            'name':                 name,
            'province':             PROVINCE_MAP.get(name, 'Unknown'),
            'total_population':     int(row[1]) if row[1] else None,
            'female_10_14':         int(row[2]) if row[2] else None,
            'female_15_19':         int(row[3]) if row[3] else None,
            'female_20_24':         int(row[4]) if row[4] else None,
            'female_25_29':         int(row[5]) if row[5] else None,
            'female_30_34':         int(row[6]) if row[6] else None,
            'female_35_39':         int(row[7]) if row[7] else None,
            'female_40_44':         int(row[8]) if row[8] else None,
            'female_45_49':         int(row[9]) if row[9] else None,
            'female_10_49_total':   f_total,
            'mean_income_lkr':      float(row[11]) if row[11] else None,
            'poverty_rate_pct':     float(row[12]) if row[12] else None,
            'female_literacy_pct':  float(row[13]) if row[13] else None,
        })

    df_districts = pd.DataFrame(districts_data)
    df_districts.to_sql('districts', engine,
                        if_exists='append', index=False)

    count = pd.read_sql(
        'SELECT COUNT(*) FROM districts', engine
    ).iloc[0, 0]
    print(f"✅ Loaded {count} districts (expected 25)")
    print(df_districts[['name', 'province',
                         'total_population',
                         'mean_income_lkr']].to_string())

except Exception as e:
    print(f"❌ District loading failed: {e}")

# ================================================================
# STEP 2: Create Empty Vulnerability Score Rows
# ================================================================
print("\n--- STEP 2: Creating Vulnerability Score Placeholders ---")

try:
    dist_ids = pd.read_sql('SELECT id, name FROM districts', engine)
    with engine.connect() as conn:
        for _, row in dist_ids.iterrows():
            conn.execute(text('''
                INSERT INTO vulnerability_scores (district_id)
                VALUES (:did)
                ON CONFLICT (district_id) DO NOTHING
            '''), {'did': int(row['id'])})
        conn.commit()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM vulnerability_scores', engine
    ).iloc[0, 0]
    print(f"✅ Created {count} vulnerability score rows (expected 25)")

except Exception as e:
    print(f"❌ Vulnerability scores setup failed: {e}")

# ================================================================
# STEP 3: Load Survey Responses
# ================================================================
print("\n--- STEP 3: Loading Survey Responses ---")

try:
    surveys = pd.read_csv('data/Menstrual Health Access Survey (Responses).csv')
    print(f"   Found {len(surveys)} rows in surveys.csv")
    print(f"   Columns: {list(surveys.columns[:5])}...")

    # Get district name to ID mapping
    dist_map = dict(
        pd.read_sql('SELECT name, id FROM districts', engine).values
    )

    # Find the district column (Q2)
    district_col = [c for c in surveys.columns if 'Q2' in c or
                    'district' in c.lower()]
    if district_col:
        surveys['district_id'] = surveys[district_col[0]].map(dist_map)
        unmapped = surveys[surveys['district_id'].isna()][
            district_col[0]].unique()
        if len(unmapped) > 0:
            print(f"   ⚠️ Unmapped districts: {unmapped}")
    else:
        surveys['district_id'] = None
        print("   ⚠️ Could not find district column")

    # Map your actual column names to database column names
    col_map = {}
    for col in surveys.columns:
        if 'Timestamp' in col:
            col_map[col] = 'submitted_at'
        elif 'Q1' in col:
            col_map[col] = 'age_group'
        elif 'Q3' in col:
            col_map[col] = 'area_type'
        elif 'Q4' in col:
            col_map[col] = 'monthly_household_income'
        elif 'Q5' in col:
            col_map[col] = 'education_level'
        elif 'Q6' in col:
            col_map[col] = 'menstruating_count'
        elif 'Q7' in col:
            col_map[col] = 'product_type'
        elif 'Q8' in col:
            col_map[col] = 'monthly_spend_lkr'
        elif 'Q9' in col:
            col_map[col] = 'purchase_location'
        elif 'Q10' in col:
            col_map[col] = 'shop_name'
        elif 'Q11' in col:
            col_map[col] = 'shop_distance'
        elif 'Q12' in col:
            col_map[col] = 'affordability_issues_6m'
        elif 'Q13' in col:
            col_map[col] = 'coping_strategy'
        elif 'Q14' in col:
            col_map[col] = 'products_too_expensive'
        elif 'Q15' in col:
            col_map[col] = 'availability_rating'
        elif 'Q16' in col:
            col_map[col] = 'comfortable_discussing'
        elif 'Q18' in col:
            col_map[col] = 'experienced_discrimination'
        elif 'Q19' in col:
            col_map[col] = 'community_impurity_belief'
        elif 'Q20' in col:
            col_map[col] = 'embarrassed_buying'
        elif 'Q21' in col:
            col_map[col] = 'age_first_learned'
        elif 'Q22' in col:
            col_map[col] = 'who_taught'
        elif 'Q23' in col:
            col_map[col] = 'received_school_education'
        elif 'Q24' in col:
            col_map[col] = 'pad_change_knowledge'
        elif 'Q25' in col:
            col_map[col] = 'knows_hygiene_risks'
        elif 'Q26' in col:
            col_map[col] = 'misses_school_work'
        elif 'Q27' in col:
            col_map[col] = 'miss_reason'
        elif 'Q30' in col:
            col_map[col] = 'daily_life_impact'

    surveys_clean = surveys.rename(columns=col_map)

    # Keep only valid columns
    valid_cols = [
        'submitted_at', 'district_id', 'age_group', 'area_type',
        'monthly_household_income', 'education_level',
        'menstruating_count', 'product_type', 'monthly_spend_lkr',
        'purchase_location', 'shop_name', 'shop_distance',
        'affordability_issues_6m', 'coping_strategy',
        'products_too_expensive', 'availability_rating',
        'comfortable_discussing', 'experienced_discrimination',
        'community_impurity_belief', 'embarrassed_buying',
        'age_first_learned', 'who_taught',
        'received_school_education', 'pad_change_knowledge',
        'knows_hygiene_risks', 'misses_school_work',
        'miss_reason', 'daily_life_impact'
    ]
    existing_cols = [c for c in valid_cols
                     if c in surveys_clean.columns]
    surveys_clean = surveys_clean[existing_cols]

    # Fix timestamp
    if 'submitted_at' in surveys_clean.columns:
        surveys_clean['submitted_at'] = pd.to_datetime(
            surveys_clean['submitted_at'], errors='coerce'
        )

    surveys_clean.to_sql('survey_responses', engine,
                         if_exists='append', index=False)

    count = pd.read_sql(
        'SELECT COUNT(*) FROM survey_responses', engine
    ).iloc[0, 0]
    print(f"✅ Loaded {count} survey responses")

except Exception as e:
    print(f"❌ Survey loading failed: {e}")
    import traceback
    traceback.print_exc()

# ================================================================
# STEP 4: Load YouTube Comments
# ================================================================
print("\n--- STEP 4: Loading YouTube Comments ---")

try:
    comments = pd.read_csv('data/YouTube_Comments_Cleaned.csv')
    print(f"   Found {len(comments)} comments")
    print(f"   Columns: {list(comments.columns)}")

    comments_clean = pd.DataFrame({
        'author':          comments.get('author', ''),
        'comment_text':    comments.get('comment', ''),
        'page_url':        comments.get('pageUrl', ''),
        'video_title':     comments.get('title', ''),
        'sentiment_label': comments.get('Sentimnet', ''),
        'topic_raw':       comments.get('Topic', ''),
    })

    # Clean empty sentiment labels
    comments_clean['sentiment_label'] = (
        comments_clean['sentiment_label']
        .astype(str)
        .str.strip()
        .replace({'nan': None, '': None})
    )

    comments_clean.to_sql('youtube_comments', engine,
                           if_exists='append', index=False)

    count = pd.read_sql(
        'SELECT COUNT(*) FROM youtube_comments', engine
    ).iloc[0, 0]
    print(f"✅ Loaded {count} YouTube comments")

except Exception as e:
    print(f"❌ YouTube comments loading failed: {e}")
    import traceback
    traceback.print_exc()

# ================================================================
# FINAL VERIFICATION
# ================================================================
print("\n" + "="*50)
print("DATABASE LOADING COMPLETE — FINAL CHECK")
print("="*50)

tables = [
    ('districts',            25),
    ('vulnerability_scores', 25),
    ('survey_responses',    347),
    ('youtube_comments',   1247),
    ('retail_outlets',        0),
    ('users',                 0),
    ('cycle_logs',            0),
]

all_good = True
for table, expected in tables:
    count = pd.read_sql(
        f'SELECT COUNT(*) FROM {table}', engine
    ).iloc[0, 0]
    status = "✅" if (expected == 0 or count >= expected * 0.95) else "⚠️"
    if status == "⚠️":
        all_good = False
    print(f"  {status} {table}: {count} rows (expected ~{expected})")

print("\n" + ("✅ ALL DATA LOADED SUCCESSFULLY!" if all_good
              else "⚠️ Some tables need attention — check above"))