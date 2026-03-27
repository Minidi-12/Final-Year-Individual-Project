-- Drop existing tables (clean start)
DROP TABLE IF EXISTS youtube_comments CASCADE;
DROP TABLE IF EXISTS district_forecasts CASCADE;
DROP TABLE IF EXISTS donation_requests CASCADE;
DROP TABLE IF EXISTS lstm_predictions CASCADE;
DROP TABLE IF EXISTS vulnerability_scores CASCADE;
DROP TABLE IF EXISTS retail_outlets CASCADE;
DROP TABLE IF EXISTS survey_responses CASCADE;
DROP TABLE IF EXISTS cycle_logs CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS districts CASCADE;

-- ================================================
-- TABLE 1: districts — 25 real districts from your Excel
-- ================================================
CREATE TABLE districts (
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(60) UNIQUE NOT NULL,
    province            VARCHAR(60),
    total_population    INTEGER,
    female_10_14        INTEGER,
    female_15_19        INTEGER,
    female_20_24        INTEGER,
    female_25_29        INTEGER,
    female_30_34        INTEGER,
    female_35_39        INTEGER,
    female_40_44        INTEGER,
    female_45_49        INTEGER,
    female_10_49_total  INTEGER,
    mean_income_lkr     NUMERIC(10,2),
    poverty_rate_pct    NUMERIC(5,2),
    female_literacy_pct NUMERIC(5,2),
    centroid            geography(Point, 4326)
);

-- ================================================
-- TABLE 2: users
-- ================================================
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    hashed_password TEXT NOT NULL,
    language_pref   CHAR(2) DEFAULT 'en',
    district_id     INTEGER REFERENCES districts(id),
    age_group       VARCHAR(20),
    income_level    VARCHAR(20),
    consent_given   BOOLEAN DEFAULT FALSE,
    is_anonymised   BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ================================================
-- TABLE 3: cycle_logs
-- ================================================
CREATE TABLE cycle_logs (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          UUID REFERENCES users(id) ON DELETE CASCADE,
    cycle_start      DATE NOT NULL,
    cycle_end        DATE,
    flow_intensity   SMALLINT CHECK (flow_intensity BETWEEN 1 AND 3),
    pain_level       SMALLINT CHECK (pain_level BETWEEN 1 AND 10),
    product_cost_lkr NUMERIC(8,2),
    symptoms         TEXT[],
    mood             VARCHAR(30),
    client_uuid      UUID UNIQUE,
    synced_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ================================================
-- TABLE 4: survey_responses — mapped to YOUR actual survey columns
-- ================================================
CREATE TABLE survey_responses (
    id                          SERIAL PRIMARY KEY,
    submitted_at                TIMESTAMPTZ,
    district_id                 INTEGER REFERENCES districts(id),

    -- Q1: Age
    age_group                   VARCHAR(20),

    -- Q3: Area type
    area_type                   VARCHAR(30),

    -- Q4: Monthly household income
    monthly_household_income    VARCHAR(40),

    -- Q5: Education
    education_level             VARCHAR(50),

    -- Q6: Menstruating individuals in household
    menstruating_count          SMALLINT,

    -- Q7: Product type
    product_type                VARCHAR(50),

    -- Q8: Monthly spend on products
    monthly_spend_lkr           VARCHAR(30),

    -- Q9: Purchase location
    purchase_location           VARCHAR(80),

    -- Q10: Shop name/location
    shop_name                   VARCHAR(120),

    -- Q11: Distance to shop
    shop_distance               VARCHAR(40),

    -- Q12: Affordability issues in past 6 months
    affordability_issues_6m     VARCHAR(50),

    -- Q13: Coping strategy when cant afford
    coping_strategy             TEXT,

    -- Q14: Products too expensive?
    products_too_expensive      VARCHAR(10),

    -- Q15: Product availability rating (1-5)
    availability_rating         SMALLINT CHECK (availability_rating BETWEEN 1 AND 5),

    -- Q16: Comfortable discussing menstruation
    comfortable_discussing      VARCHAR(10),

    -- Q17: Avoided activities
    avoids_religious_places     VARCHAR(10),
    avoids_religious_ceremonies VARCHAR(10),
    avoids_social_events        VARCHAR(10),
    avoids_physical_exercise    VARCHAR(10),

    -- Q18: Experienced discrimination
    experienced_discrimination  VARCHAR(10),

    -- Q19: Community impurity belief
    community_impurity_belief   VARCHAR(10),

    -- Q20: Embarrassed buying products
    embarrassed_buying          VARCHAR(10),

    -- Q21: Age first learned about menstruation
    age_first_learned           VARCHAR(20),

    -- Q22: Who taught first
    who_taught                  VARCHAR(50),

    -- Q23: Received school education
    received_school_education   VARCHAR(10),

    -- Q24: Pad change frequency knowledge
    pad_change_knowledge        VARCHAR(50),

    -- Q25: Knows poor hygiene causes problems
    knows_hygiene_risks         VARCHAR(10),

    -- Q26: Misses school/work
    misses_school_work          VARCHAR(10),

    -- Q27: Reason for missing
    miss_reason                 TEXT,

    -- Q30: Daily life impact severity
    daily_life_impact           VARCHAR(30)
);

-- ================================================
-- TABLE 5: retail_outlets
-- ================================================
CREATE TABLE retail_outlets (
    id                 SERIAL PRIMARY KEY,
    district_id        INTEGER REFERENCES districts(id),
    shop_name          VARCHAR(120),
    address            TEXT,
    location           geography(Point, 4326),
    avg_pad_price_lkr  NUMERIC(8,2),
    brand_count        SMALLINT,
    area_type          VARCHAR(20),
    verified_date      DATE,
    geocode_confidence NUMERIC(4,2),
    geocode_stage      VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_retail_loc
ON retail_outlets USING GIST(location);

-- ================================================
-- TABLE 6: vulnerability_scores
-- ================================================
CREATE TABLE vulnerability_scores (
    id                  SERIAL PRIMARY KEY,
    district_id         INTEGER REFERENCES districts(id) UNIQUE,
    economic_score      NUMERIC(5,4),
    accessibility_score NUMERIC(5,4),
    knowledge_score     NUMERIC(5,4),
    stigma_score_idx    NUMERIC(5,4),
    composite_mhvs      NUMERIC(5,4),
    cluster_label       SMALLINT,
    vulnerability_level VARCHAR(10),
    hotspot_class       VARCHAR(30),
    gi_z_score          NUMERIC(6,4),
    calculated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ================================================
-- TABLE 7: lstm_predictions
-- ================================================
CREATE TABLE lstm_predictions (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID REFERENCES users(id),
    predicted_start_1 DATE,
    predicted_start_2 DATE,
    predicted_start_3 DATE,
    predicted_pain_1  NUMERIC(3,1),
    predicted_pain_2  NUMERIC(3,1),
    predicted_pain_3  NUMERIC(3,1),
    is_personalized   BOOLEAN DEFAULT FALSE,
    model_version     VARCHAR(20),
    confidence_score  NUMERIC(4,3),
    generated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ================================================
-- TABLE 8: donation_requests
-- ================================================
CREATE TABLE donation_requests (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID REFERENCES users(id),
    district_id     INTEGER REFERENCES districts(id),
    product_type    VARCHAR(50),
    quantity_needed SMALLINT,
    urgency_score   SMALLINT CHECK (urgency_score BETWEEN 1 AND 5),
    priority_score  NUMERIC(5,4),
    status          VARCHAR(20) DEFAULT 'QUEUED',
    fraud_flag      BOOLEAN DEFAULT FALSE,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    fulfilled_at    TIMESTAMPTZ
);

-- ================================================
-- TABLE 9: district_forecasts
-- ================================================
CREATE TABLE district_forecasts (
    id             SERIAL PRIMARY KEY,
    district_id    INTEGER REFERENCES districts(id),
    forecast_date  DATE NOT NULL,
    predicted_mhvs NUMERIC(5,4),
    lower_bound    NUMERIC(5,4),
    upper_bound    NUMERIC(5,4),
    horizon_months SMALLINT,
    model_version  VARCHAR(20),
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

-- ================================================
-- TABLE 10: youtube_comments — mapped to YOUR actual columns
-- ================================================
CREATE TABLE youtube_comments (
    id               SERIAL PRIMARY KEY,
    author           VARCHAR(120),
    comment_text     TEXT NOT NULL,
    page_url         TEXT,
    video_title      VARCHAR(255),

    -- Sentiment (from your Sentiment column)
    sentiment_label  VARCHAR(10),        -- 'Positive', 'Negative', 'Neutral'
    sentiment_score  NUMERIC(5,4),       -- VADER compound score (computed in Week 3)
    positive_score   NUMERIC(5,4),
    negative_score   NUMERIC(5,4),
    neutral_score    NUMERIC(5,4),

    -- LDA Topic (from your Topic column)
    topic_raw        VARCHAR(60),        -- raw topic from your sheet
    dominant_topic   SMALLINT,           -- topic number 0-8
    topic_label      VARCHAR(80),        -- human readable label
    topic_confidence NUMERIC(5,4),

    collected_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sentiment
ON youtube_comments(sentiment_label);

CREATE INDEX IF NOT EXISTS idx_topic
ON youtube_comments(dominant_topic);

-- Fix all VARCHAR columns that are too small
ALTER TABLE survey_responses
    ALTER COLUMN age_group                   TYPE TEXT,
    ALTER COLUMN area_type                   TYPE TEXT,
    ALTER COLUMN monthly_household_income    TYPE TEXT,
    ALTER COLUMN education_level             TYPE TEXT,
    ALTER COLUMN menstruating_count          TYPE TEXT,
    ALTER COLUMN product_type                TYPE TEXT,
    ALTER COLUMN monthly_spend_lkr           TYPE TEXT,
    ALTER COLUMN purchase_location           TYPE TEXT,
    ALTER COLUMN shop_name                   TYPE TEXT,
    ALTER COLUMN shop_distance               TYPE TEXT,
    ALTER COLUMN affordability_issues_6m     TYPE TEXT,
    ALTER COLUMN coping_strategy             TYPE TEXT,
    ALTER COLUMN products_too_expensive      TYPE TEXT,
    ALTER COLUMN comfortable_discussing      TYPE TEXT,
    ALTER COLUMN avoids_religious_places     TYPE TEXT,
    ALTER COLUMN avoids_religious_ceremonies TYPE TEXT,
    ALTER COLUMN avoids_social_events        TYPE TEXT,
    ALTER COLUMN avoids_physical_exercise    TYPE TEXT,
    ALTER COLUMN experienced_discrimination  TYPE TEXT,
    ALTER COLUMN community_impurity_belief   TYPE TEXT,
    ALTER COLUMN embarrassed_buying          TYPE TEXT,
    ALTER COLUMN age_first_learned           TYPE TEXT,
    ALTER COLUMN who_taught                  TYPE TEXT,
    ALTER COLUMN received_school_education   TYPE TEXT,
    ALTER COLUMN pad_change_knowledge        TYPE TEXT,
    ALTER COLUMN knows_hygiene_risks         TYPE TEXT,
    ALTER COLUMN misses_school_work          TYPE TEXT,
    ALTER COLUMN miss_reason                 TYPE TEXT,
    ALTER COLUMN daily_life_impact           TYPE TEXT;