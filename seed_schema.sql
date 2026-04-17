-- OpenPostings seed schema (tables only)
-- This is just so someone can quickly understand the database schema. 
PRAGMA foreign_keys = ON;

-- McpSettings
CREATE TABLE IF NOT EXISTS McpSettings (
      id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
      enabled INTEGER NOT NULL DEFAULT 0,
      preferred_agent_name TEXT NOT NULL DEFAULT 'OpenPostings Agent',
      agent_login_email TEXT NOT NULL DEFAULT '',
      mfa_login_email TEXT NOT NULL DEFAULT '',
      mfa_login_notes TEXT NOT NULL DEFAULT '',
      dry_run_only INTEGER NOT NULL DEFAULT 1,
      require_final_approval INTEGER NOT NULL DEFAULT 1,
      max_applications_per_run INTEGER NOT NULL DEFAULT 10,
      preferred_search TEXT NOT NULL DEFAULT '',
      preferred_remote TEXT NOT NULL DEFAULT 'all',
      preferred_industries TEXT NOT NULL DEFAULT '[]',
      preferred_states TEXT NOT NULL DEFAULT '[]',
      preferred_counties TEXT NOT NULL DEFAULT '[]',
      instructions_for_agent TEXT NOT NULL DEFAULT '',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    , agent_login_password TEXT NOT NULL DEFAULT '');

-- PersonalInformation
CREATE TABLE IF NOT EXISTS PersonalInformation (
	first_name TEXT NOT NULL,
	middle_name TEXT NOT NULL,
	last_name TEXT NOT NULL,
	email TEXT NOT NULL,
	phone_number TEXT NOT NULL,
	address TEXT NOT NULL,
	linkedin_url TEXT NOT NULL,
	github_url TEXT NOT NULL,
	portfolio_url TEXT NOT NULL,
	resume_file_path TEXT NOT NULL,
	projects_portfolio_file_path TEXT NOT NULL,
	certifications_folder_path TEXT NOT NULL,
	ethnicity TEXT NOT NULL,
	gender TEXT NOT NULL,
	age INTEGER NOT NULL,
	veteran_status TEXT NOT NULL,
	disability_status TEXT NOT NULL,
	education_level TEXT NOT NULL
, years_of_experience INTEGER);

-- Postings
CREATE TABLE IF NOT EXISTS Postings (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_name TEXT NOT NULL,
      position_name TEXT NOT NULL,
      job_posting_url TEXT NOT NULL UNIQUE,
      posting_date TEXT
    , last_seen_epoch INTEGER);

-- SyncServiceSettings
CREATE TABLE IF NOT EXISTS SyncServiceSettings (
  id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
  ats_request_queue_concurrency INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- blocked_companies
CREATE TABLE IF NOT EXISTS blocked_companies (
  normalized_company_name TEXT NOT NULL PRIMARY KEY,
  company_name TEXT NOT NULL,
  blocked_at_epoch INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_blocked_companies_company_name
  ON blocked_companies(company_name);

-- application_attribution
CREATE TABLE IF NOT EXISTS application_attribution (
      application_id INTEGER NOT NULL PRIMARY KEY,
      applied_by_type TEXT NOT NULL,
      applied_by_label TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

-- applications
CREATE TABLE IF NOT EXISTS applications (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	company_id INTEGER NOT NULL,
	position_name TEXT NOT NULL,
	application_date INTEGER NOT NULL,
	status TEXT
);

-- companies
CREATE TABLE IF NOT EXISTS companies (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	company_name TEXT NOT NULL,
	url_string TEXT NOT NULL,
	ATS_name TEXT NOT NULL
);

-- job_industry_categories
CREATE TABLE IF NOT EXISTS job_industry_categories (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      industry_key TEXT NOT NULL UNIQUE,
      industry_label TEXT NOT NULL,
      priority INTEGER NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

-- job_position_industry
CREATE TABLE IF NOT EXISTS job_position_industry (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      job_title TEXT NOT NULL,
      normalized_job_title TEXT NOT NULL UNIQUE,
      industry_key TEXT NOT NULL,
      industry_label TEXT NOT NULL,
      matched_rules TEXT NOT NULL,
      confidence_score REAL NOT NULL,
      rule_version TEXT NOT NULL DEFAULT 'rule_bootstrap_v4',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (industry_key) REFERENCES job_industry_categories(industry_key)
    );

-- lost_and_found
CREATE TABLE IF NOT EXISTS lost_and_found(rootpgno INTEGER, pgno INTEGER, nfield INTEGER, id INTEGER, c0, c1, c2, c3, c4, c5);

-- posting_application_state
CREATE TABLE IF NOT EXISTS posting_application_state (
      job_posting_url TEXT NOT NULL PRIMARY KEY,
      applied INTEGER NOT NULL DEFAULT 0,
      applied_by_type TEXT NOT NULL,
      applied_by_label TEXT NOT NULL,
      applied_at_epoch INTEGER,
      last_application_id INTEGER,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    , ignored INTEGER NOT NULL DEFAULT 0, ignored_at_epoch INTEGER, ignored_by_label TEXT NOT NULL DEFAULT '');

-- state_location_index
CREATE TABLE IF NOT EXISTS state_location_index (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      location_type TEXT NOT NULL CHECK (location_type IN ('city', 'county')),
      state_usps TEXT NOT NULL,
      state_geoid TEXT,
      location_geoid TEXT NOT NULL,
      ansicode TEXT,
      location_name TEXT NOT NULL,
      search_location_name TEXT NOT NULL,
      normalized_location_name TEXT NOT NULL,
      normalized_search_location_name TEXT NOT NULL,
      lsad_code TEXT,
      funcstat TEXT,
      aland INTEGER,
      awater INTEGER,
      aland_sqmi REAL,
      awater_sqmi REAL,
      intptlat REAL,
      intptlong REAL,
      source_file TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(location_type, location_geoid)
    );
