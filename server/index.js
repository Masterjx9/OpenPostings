const cors = require("cors");
const express = require("express");
const path = require("path");
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");

const PORT = Number(process.env.PORT || 8787);
const DB_PATH = process.env.DB_PATH || path.resolve(__dirname, "..", "jobs.db");
const SYNC_INTERVAL_MS = Number(process.env.SYNC_INTERVAL_MS || 10 * 60 * 1000);
const SYNC_WORKER_CONCURRENCY_RAW = Number(process.env.SYNC_WORKER_CONCURRENCY || 4);
const SYNC_WORKER_CONCURRENCY =
  Number.isFinite(SYNC_WORKER_CONCURRENCY_RAW) && SYNC_WORKER_CONCURRENCY_RAW > 0
    ? Math.floor(SYNC_WORKER_CONCURRENCY_RAW)
    : 4;
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || 12000);
const ATS_REQUEST_QUEUE_CONCURRENCY_RAW = Number(process.env.ATS_REQUEST_QUEUE_CONCURRENCY || 1);
const ATS_REQUEST_QUEUE_CONCURRENCY_DEFAULT =
  Number.isFinite(ATS_REQUEST_QUEUE_CONCURRENCY_RAW) && ATS_REQUEST_QUEUE_CONCURRENCY_RAW > 0
    ? Math.floor(ATS_REQUEST_QUEUE_CONCURRENCY_RAW)
    : 1;
const MIN_ATS_REQUEST_QUEUE_CONCURRENCY = 1;
const MAX_ATS_REQUEST_QUEUE_CONCURRENCY = 20;
const POSTING_TTL_SECONDS = Number(process.env.POSTING_TTL_SECONDS || 24 * 60 * 60);
const SYNC_POSTING_FLUSH_BATCH_SIZE = Number(process.env.SYNC_POSTING_FLUSH_BATCH_SIZE || 200);
const WORKDAY_PAGE_SIZE = 20;
const ULTIPRO_PAGE_SIZE = 50;
const MAX_PAGES_PER_COMPANY = 25;
const LOCALE_SEGMENT_REGEX = /^[a-z]{2}(?:-[a-z]{2})?$/i;
const WORKDAY_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ASHBY_API_URL = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams";
const ASHBY_RATE_LIMIT_WAIT_MS = 60 * 1000;
const GREENHOUSE_API_URL_BASE = "https://boards-api.greenhouse.io/v1/boards";
const GREENHOUSE_RATE_LIMIT_WAIT_MS = 60 * 1000;
const LEVER_API_URL_BASE = "https://api.lever.co/v0/postings";
const LEVER_RATE_LIMIT_WAIT_MS = 60 * 1000;
const RECRUITEE_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ULTIPRO_RATE_LIMIT_WAIT_MS = 60 * 1000;
const TALEO_RATE_LIMIT_WAIT_MS = 60 * 1000;
const JOBVITE_RATE_LIMIT_WAIT_MS = 60 * 1000;
const APPLICANTPRO_RATE_LIMIT_WAIT_MS = 60 * 1000;
const APPLYTOJOB_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ICIMS_RATE_LIMIT_WAIT_MS = 60 * 1000;
const THEAPPLICANTMANAGER_RATE_LIMIT_WAIT_MS = 60 * 1000;
const BREEZY_RATE_LIMIT_WAIT_MS = 60 * 1000;
const ZOHO_RATE_LIMIT_WAIT_MS = 60 * 1000;
const APPLICANTAI_RATE_LIMIT_WAIT_MS = 60 * 1000;
const CAREERPLUG_RATE_LIMIT_WAIT_MS = 60 * 1000;
const BAMBOOHR_RATE_LIMIT_WAIT_MS = 60 * 1000;
const CAREERPUCK_RATE_LIMIT_WAIT_MS = 60 * 1000;
const FOUNTAIN_RATE_LIMIT_WAIT_MS = 60 * 1000;
const GETRO_RATE_LIMIT_WAIT_MS = 60 * 1000;
const HRMDIRECT_RATE_LIMIT_WAIT_MS = 60 * 1000;
const TALENTLYFT_RATE_LIMIT_WAIT_MS = 60 * 1000;
const TALEXIO_RATE_LIMIT_WAIT_MS = 60 * 1000;
const TEAMTAILOR_RATE_LIMIT_WAIT_MS = 60 * 1000;
const MANATAL_RATE_LIMIT_WAIT_MS = 60 * 1000;
const GEM_RATE_LIMIT_WAIT_MS = 60 * 1000;
const JOBAPS_RATE_LIMIT_WAIT_MS = 60 * 1000;
const JOIN_RATE_LIMIT_WAIT_MS = 60 * 1000;
const TALENTREEF_RATE_LIMIT_WAIT_MS = 60 * 1000;
const SAPHRCLOUD_RATE_LIMIT_WAIT_MS = 60 * 1000;
const SAPHRCLOUD_LOCALE_CANDIDATES = Object.freeze(["en_US", "en_GB"]);
const ASHBY_QUERY = `
  query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
    jobBoard: jobBoardWithTeams(
      organizationHostedJobsPageName: $organizationHostedJobsPageName
    ) {
      teams {
        id
        name
        externalName
        parentTeamId
        __typename
      }
      jobPostings {
        id
        title
        teamId
        locationId
        locationName
        workplaceType
        employmentType
        secondaryLocations {
          ...JobPostingSecondaryLocationParts
          __typename
        }
        compensationTierSummary
        __typename
      }
      __typename
    }
  }

  fragment JobPostingSecondaryLocationParts on JobPostingSecondaryLocation {
    locationId
    locationName
    __typename
  }
`;

let db;
let wordIndustryCoverageCache = null;
let phraseNgramIndustryCoverageCache = null;
let syncPromise = null;
let postingLocationByJobUrl = new Map();
const atsRateLimitStateByKey = new Map();
let atsRequestQueueConcurrency = ATS_REQUEST_QUEUE_CONCURRENCY_DEFAULT;
const syncStatus = {
  running: false,
  started_at: null,
  last_sync_at: null,
  last_sync_summary: null,
  last_error: null,
  progress: null
};
const PERSONAL_INFORMATION_FIELDS = [
  "first_name",
  "middle_name",
  "last_name",
  "email",
  "phone_number",
  "address",
  "linkedin_url",
  "github_url",
  "portfolio_url",
  "resume_file_path",
  "projects_portfolio_file_path",
  "certifications_folder_path",
  "ethnicity",
  "gender",
  "age",
  "veteran_status",
  "disability_status",
  "education_level",
  "years_of_experience"
];
const PERSONAL_INFORMATION_DEFAULTS = {
  first_name: "",
  middle_name: "",
  last_name: "",
  email: "",
  phone_number: "",
  address: "",
  linkedin_url: "",
  github_url: "",
  portfolio_url: "",
  resume_file_path: "",
  projects_portfolio_file_path: "",
  certifications_folder_path: "",
  ethnicity: "",
  gender: "",
  age: 0,
  veteran_status: "",
  disability_status: "",
  education_level: "",
  years_of_experience: 0
};
const GENERIC_TITLE_LIKE_PARTS = new Set([
  "and",
  "for",
  "with",
  "from",
  "the",
  "manager",
  "assistant",
  "associate",
  "specialist",
  "coordinator",
  "director",
  "officer",
  "analyst",
  "consultant",
  "lead",
  "senior",
  "junior",
  "staff",
  "team",
  "services",
  "service",
  "operations",
  "operation",
  "support"
]);
const WEAK_INDUSTRY_LIKE_PARTS = new Set([
  ...GENERIC_TITLE_LIKE_PARTS,
  "account",
  "accounts",
  "representative",
  "executive",
  "management",
  "area",
  "group",
  "international",
  "care",
  "inside",
  "outside",
  "hourly",
  "commission",
  "anywhere",
  "can",
  "small",
  "planning",
  "compliance",
  "core",
  "safety",
  "import",
  "export",
  "brand",
  "ambassador",
  "customer",
  "business",
  "field",
  "division",
  "product"
]);
const IT_SOFTWARE_INDUSTRY_KEY = "information_technology_software";
const SALES_BUSINESS_INDUSTRY_KEY = "sales_business_development";
const IT_TECH_ANCHOR_PARTS = new Set([
  "software",
  "developer",
  "development",
  "engineer",
  "engineering",
  "devops",
  "platform",
  "cloud",
  "security",
  "cybersecurity",
  "cyber",
  "infrastructure",
  "network",
  "systems",
  "system",
  "administrator",
  "database",
  "sql",
  "data",
  "analytics",
  "architect",
  "automation",
  "backend",
  "frontend",
  "fullstack",
  "application",
  "applications",
  "qa",
  "test",
  "testing",
  "machine",
  "learning",
  "mlops",
  "ai"
]);
const IT_HIGH_SIGNAL_ANCHOR_PARTS = new Set([
  "software",
  "developer",
  "development",
  "engineer",
  "engineering",
  "devops",
  "platform",
  "cloud",
  "security",
  "cybersecurity",
  "cyber",
  "infrastructure",
  "network",
  "systems",
  "system",
  "administrator",
  "database",
  "sql",
  "architect",
  "automation",
  "backend",
  "frontend",
  "fullstack",
  "mlops",
  "machine",
  "learning",
  "ai"
]);
const IT_SALES_GTM_ROLE_REGEX =
  /\b(account executive|account manager|business development|brand ambassador|go[\s-]?to[\s-]?market|gtm|inside sales|outside sales|sales representative|territory manager|partnerships?|sales(?!force\b))\b/i;
const SALES_EXCLUSIVE_ROLE_REGEX =
  /\b(account executive|account manager|business development|brand ambassador|inside sales|outside sales|sales representative|sales manager|sales director|sales consultant|sales specialist|sales associate|sales advisor|presales?|telesales|territory manager|channel sales|partner sales|salesperson|salesman|salesworker|sales(?!force\b))\b/i;
const STATE_CODE_TO_NAME = {
  AL: "alabama",
  AK: "alaska",
  AZ: "arizona",
  AR: "arkansas",
  CA: "california",
  CO: "colorado",
  CT: "connecticut",
  DE: "delaware",
  FL: "florida",
  GA: "georgia",
  HI: "hawaii",
  ID: "idaho",
  IL: "illinois",
  IN: "indiana",
  IA: "iowa",
  KS: "kansas",
  KY: "kentucky",
  LA: "louisiana",
  ME: "maine",
  MD: "maryland",
  MA: "massachusetts",
  MI: "michigan",
  MN: "minnesota",
  MS: "mississippi",
  MO: "missouri",
  MT: "montana",
  NE: "nebraska",
  NV: "nevada",
  NH: "new hampshire",
  NJ: "new jersey",
  NM: "new mexico",
  NY: "new york",
  NC: "north carolina",
  ND: "north dakota",
  OH: "ohio",
  OK: "oklahoma",
  OR: "oregon",
  PA: "pennsylvania",
  RI: "rhode island",
  SC: "south carolina",
  SD: "south dakota",
  TN: "tennessee",
  TX: "texas",
  UT: "utah",
  VT: "vermont",
  VA: "virginia",
  WA: "washington",
  WV: "west virginia",
  WI: "wisconsin",
  WY: "wyoming",
  DC: "district of columbia"
};
const APPLICATION_STATUS_OPTIONS = new Set([
  "applied",
  "interview scheduled",
  "awaiting response",
  "offer received",
  "withdrawn",
  "denied"
]);
const MCP_REMOTE_OPTIONS = new Set(["all", "remote", "non_remote"]);
const ATS_FILTER_OPTIONS = new Set([
  "workday",
  "ashby",
  "greenhouse",
  "lever",
  "recruitee",
  "ultipro",
  "taleo",
  "jobvite",
  "applicantpro",
  "applytojob",
  "icims",
  "theapplicantmanager",
  "breezy",
  "zoho",
  "applicantai",
  "careerplug",
  "bamboohr",
  "manatal",
  "careerpuck",
  "fountain",
  "getro",
  "hrmdirect",
  "talentlyft",
  "talexio",
  "teamtailor",
  "gem",
  "jobaps",
  "join",
  "talentreef",
  "saphrcloud"
]);
const POSTING_SORT_OPTIONS = new Set(["recent", "company_asc"]);
const MCP_SETTINGS_DEFAULTS = {
  enabled: false,
  preferred_agent_name: "OpenPostings Agent",
  agent_login_email: "",
  agent_login_password: "",
  mfa_login_email: "",
  mfa_login_notes: "",
  dry_run_only: true,
  require_final_approval: true,
  max_applications_per_run: 10,
  preferred_search: "",
  preferred_remote: "all",
  preferred_industries: [],
  preferred_states: [],
  preferred_counties: [],
  instructions_for_agent: ""
};
const SYNC_SERVICE_SETTINGS_DEFAULTS = {
  ats_request_queue_concurrency: ATS_REQUEST_QUEUE_CONCURRENCY_DEFAULT
};
const PHRASE_NGRAM_INDUSTRY_COVERAGE_THRESHOLD = 2;
const FALLBACK_WORD_INDUSTRY_COVERAGE_THRESHOLD = 2;
const MIN_INDUSTRY_FALLBACK_WORD_COUNT = 3;
const MIN_INDUSTRY_PHRASE_NGRAM_COUNT = 2;

function nowEpochSeconds() {
  return Math.floor(Date.now() / 1000);
}

function normalizeLikeText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function parseCsvParam(value) {
  return String(value || "")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseJsonArray(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  try {
    const parsed = JSON.parse(String(value || "[]"));
    if (!Array.isArray(parsed)) return [];
    return parsed.map((item) => String(item || "").trim()).filter(Boolean);
  } catch {
    return [];
  }
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function createLikeParts(value) {
  const normalized = normalizeLikeText(value);
  if (!normalized) return [];
  return normalized
    .split(/[^a-z0-9]+/)
    .map((part) => part.trim())
    .filter((part) => part.length >= 3 && !GENERIC_TITLE_LIKE_PARTS.has(part));
}

function buildWordNgrams(words, minSize = 2, maxSize = 3) {
  const source = Array.isArray(words) ? words : [];
  const ngrams = [];
  for (let size = minSize; size <= maxSize; size += 1) {
    if (source.length < size) continue;
    for (let index = 0; index <= source.length - size; index += 1) {
      const gram = source.slice(index, index + size).join(" ").trim();
      if (gram) ngrams.push(gram);
    }
  }
  return ngrams;
}

function normalizeCountyName(value) {
  return normalizeLikeText(value)
    .replace(/\b(county|parish|borough|census area|municipality)\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseCountyFilters(values) {
  const parsed = [];
  for (const rawValue of values) {
    const value = String(rawValue || "").trim();
    if (!value) continue;

    if (value.includes("|")) {
      const [stateRaw, countyRaw] = value.split("|");
      const stateCode = String(stateRaw || "").trim().toUpperCase();
      const countyLikePart = normalizeCountyName(countyRaw);
      if (!countyLikePart) continue;
      parsed.push({ stateCode, countyLikePart });
      continue;
    }

    const countyLikePart = normalizeCountyName(value);
    if (!countyLikePart) continue;
    parsed.push({ stateCode: "", countyLikePart });
  }
  return parsed;
}

function hasStateLikeMatch(locationText, stateCode) {
  const code = String(stateCode || "").trim().toUpperCase();
  if (!code) return false;

  const upperLocation = String(locationText || "").toUpperCase();
  const codeRegex = new RegExp(`(^|[^A-Z])${escapeRegExp(code)}([^A-Z]|$)`);
  if (codeRegex.test(upperLocation)) return true;

  const stateName = STATE_CODE_TO_NAME[code];
  if (!stateName) return false;
  return normalizeLikeText(locationText).includes(stateName);
}

function isRemoteLocation(locationText) {
  const normalized = normalizeLikeText(locationText);
  if (!normalized) return false;
  return (
    normalized.includes("remote") ||
    normalized.includes("work from home") ||
    normalized.includes("wfh") ||
    normalized.includes("hybrid")
  );
}

async function buildIndustryMatchersByKey(industryKeys) {
  if (!Array.isArray(industryKeys) || industryKeys.length === 0) {
    return new Map();
  }

  const [wordIndustryCoverage, phraseNgramIndustryCoverage] = await Promise.all([
    getWordIndustryCoverageMap(),
    getPhraseNgramIndustryCoverageMap()
  ]);

  const placeholders = industryKeys.map(() => "?").join(", ");
  let rows = [];
  try {
    rows = await db.all(
      `
        SELECT industry_key, normalized_job_title
        FROM job_position_industry
        WHERE industry_key IN (${placeholders});
      `,
      industryKeys
    );
  } catch {
    return new Map();
  }

  const byIndustry = new Map();
  for (const key of industryKeys) {
    byIndustry.set(key, {
      exactTitles: new Set(),
      phraseNgrams: new Set(),
      fallbackWords: new Set(),
      wordCounts: new Map(),
      phraseCounts: new Map()
    });
  }

  for (const row of rows) {
    const key = String(row?.industry_key || "").trim();
    if (!key || !byIndustry.has(key)) continue;
    const normalizedTitle = normalizeLikeText(row?.normalized_job_title);
    const words = createLikeParts(normalizedTitle);
    const target = byIndustry.get(key);
    if (normalizedTitle) {
      target.exactTitles.add(normalizedTitle);
    }

    for (const word of new Set(words)) {
      target.wordCounts.set(word, (target.wordCounts.get(word) || 0) + 1);
    }

    for (const ngram of new Set(buildWordNgrams(words, 2, 3))) {
      target.phraseCounts.set(ngram, (target.phraseCounts.get(ngram) || 0) + 1);
    }
  }

  const finalized = new Map();
  for (const [industryKey, matcher] of byIndustry.entries()) {
    const fallbackWords = new Set();
    for (const [word, count] of matcher.wordCounts.entries()) {
      if (count < MIN_INDUSTRY_FALLBACK_WORD_COUNT) continue;
      if (isWeakFallbackWord(word, wordIndustryCoverage)) continue;
      fallbackWords.add(word);
    }

    const phraseNgrams = new Set();
    for (const [ngram, count] of matcher.phraseCounts.entries()) {
      if (count < MIN_INDUSTRY_PHRASE_NGRAM_COUNT) continue;
      if (isWeakPhraseNgram(ngram, phraseNgramIndustryCoverage)) continue;
      phraseNgrams.add(ngram);
    }

    finalized.set(industryKey, {
      exactTitles: matcher.exactTitles,
      phraseNgrams,
      fallbackWords
    });
  }

  return finalized;
}

async function getWordIndustryCoverageMap() {
  if (wordIndustryCoverageCache instanceof Map) {
    return wordIndustryCoverageCache;
  }

  let rows = [];
  try {
    rows = await db.all(
      `
        SELECT industry_key, normalized_job_title
        FROM job_position_industry;
      `
    );
  } catch {
    wordIndustryCoverageCache = new Map();
    return wordIndustryCoverageCache;
  }

  const wordIndustrySets = new Map();
  for (const row of rows) {
    const industryKey = String(row?.industry_key || "").trim();
    if (!industryKey) continue;

    const words = new Set(createLikeParts(row?.normalized_job_title));
    for (const word of words) {
      if (!wordIndustrySets.has(word)) {
        wordIndustrySets.set(word, new Set());
      }
      wordIndustrySets.get(word).add(industryKey);
    }
  }

  const coverageMap = new Map();
  for (const [word, keys] of wordIndustrySets.entries()) {
    coverageMap.set(word, keys.size);
  }

  wordIndustryCoverageCache = coverageMap;
  return coverageMap;
}

async function getPhraseNgramIndustryCoverageMap() {
  if (phraseNgramIndustryCoverageCache instanceof Map) {
    return phraseNgramIndustryCoverageCache;
  }

  let rows = [];
  try {
    rows = await db.all(
      `
        SELECT industry_key, normalized_job_title
        FROM job_position_industry;
      `
    );
  } catch {
    phraseNgramIndustryCoverageCache = new Map();
    return phraseNgramIndustryCoverageCache;
  }

  const ngramIndustrySets = new Map();
  for (const row of rows) {
    const industryKey = String(row?.industry_key || "").trim();
    if (!industryKey) continue;

    const words = createLikeParts(row?.normalized_job_title);
    const ngrams = new Set(buildWordNgrams(words, 2, 3));
    for (const ngram of ngrams) {
      if (!ngramIndustrySets.has(ngram)) {
        ngramIndustrySets.set(ngram, new Set());
      }
      ngramIndustrySets.get(ngram).add(industryKey);
    }
  }

  const coverageMap = new Map();
  for (const [ngram, keys] of ngramIndustrySets.entries()) {
    coverageMap.set(ngram, keys.size);
  }

  phraseNgramIndustryCoverageCache = coverageMap;
  return coverageMap;
}

function isWeakFallbackWord(word, wordIndustryCoverage) {
  if (!word) return true;
  if (WEAK_INDUSTRY_LIKE_PARTS.has(word)) return true;
  const industryCoverage = Number(wordIndustryCoverage?.get(word) || 0);
  return industryCoverage >= FALLBACK_WORD_INDUSTRY_COVERAGE_THRESHOLD;
}

function isWeakPhraseNgram(ngram, phraseNgramIndustryCoverage) {
  if (!ngram) return true;
  const parts = ngram.split(" ").map((part) => part.trim()).filter(Boolean);
  if (parts.length < 2) return true;
  if (parts.every((part) => WEAK_INDUSTRY_LIKE_PARTS.has(part))) return true;
  const industryCoverage = Number(phraseNgramIndustryCoverage?.get(ngram) || 0);
  return industryCoverage >= PHRASE_NGRAM_INDUSTRY_COVERAGE_THRESHOLD;
}

function rowMatchesIndustryLikeParts(positionName, selectedIndustryKeys, industryMatchersByKey) {
  if (!Array.isArray(selectedIndustryKeys) || selectedIndustryKeys.length === 0) return true;
  if (!(industryMatchersByKey instanceof Map) || industryMatchersByKey.size === 0) return false;

  const titleText = String(positionName || "");
  const selectedKeySet = new Set(
    selectedIndustryKeys.map((key) => String(key || "").trim().toLowerCase()).filter(Boolean)
  );
  const isSalesExclusiveRole = SALES_EXCLUSIVE_ROLE_REGEX.test(titleText);
  if (isSalesExclusiveRole && !selectedKeySet.has(SALES_BUSINESS_INDUSTRY_KEY)) {
    return false;
  }

  const normalizedPosition = normalizeLikeText(positionName);
  const postingWords = createLikeParts(positionName);
  if (postingWords.length === 0) return false;
  const postingWordSet = new Set(postingWords);
  const postingPhraseSet = new Set(buildWordNgrams(postingWords, 2, 3));

  for (const industryKey of selectedIndustryKeys) {
    const matcher = industryMatchersByKey.get(industryKey);
    const exactTitles = matcher?.exactTitles;
    const phraseNgrams = matcher?.phraseNgrams;
    const fallbackWords = matcher?.fallbackWords;
    const hasMatcherData =
      exactTitles instanceof Set || phraseNgrams instanceof Set || fallbackWords instanceof Set;
    if (!hasMatcherData) continue;

    if (exactTitles instanceof Set && normalizedPosition && exactTitles.has(normalizedPosition)) {
      if (industryKey === IT_SOFTWARE_INDUSTRY_KEY && IT_SALES_GTM_ROLE_REGEX.test(titleText)) {
        continue;
      }

      const hasStrongPhrase =
        phraseNgrams instanceof Set &&
        Array.from(postingPhraseSet).some((postingPhrase) => phraseNgrams.has(postingPhrase));
      const hasStrongWord =
        fallbackWords instanceof Set &&
        Array.from(postingWordSet).some((word) => fallbackWords.has(word));
      if (hasStrongPhrase || hasStrongWord) {
        return true;
      }
      if (
        industryKey === IT_SOFTWARE_INDUSTRY_KEY &&
        Array.from(postingWordSet).some((word) => IT_HIGH_SIGNAL_ANCHOR_PARTS.has(word))
      ) {
        return true;
      }
    }

    if (industryKey === IT_SOFTWARE_INDUSTRY_KEY) {
      if (IT_SALES_GTM_ROLE_REGEX.test(titleText)) continue;
      const hasTechAnchor = Array.from(postingWordSet).some((part) => IT_TECH_ANCHOR_PARTS.has(part));
      if (!hasTechAnchor) continue;
    }

    if (phraseNgrams instanceof Set && phraseNgrams.size > 0) {
      for (const postingPhrase of postingPhraseSet) {
        if (phraseNgrams.has(postingPhrase)) {
          return true;
        }
      }
    }

    if (fallbackWords instanceof Set && fallbackWords.size > 0) {
      for (const word of postingWordSet) {
        if (fallbackWords.has(word)) {
          if (
            industryKey !== IT_SOFTWARE_INDUSTRY_KEY ||
            postingWordSet.size === 1 ||
            IT_HIGH_SIGNAL_ANCHOR_PARTS.has(word)
          ) {
            return true;
          }
        }
      }
    }

    if (industryKey === IT_SOFTWARE_INDUSTRY_KEY) {
      for (const word of postingWordSet) {
        if (IT_HIGH_SIGNAL_ANCHOR_PARTS.has(word) && fallbackWords instanceof Set && fallbackWords.has(word)) {
          return true;
        }
      }
    }
  }

  return false;
}
function rowMatchesLocationFilters(locationText, selectedStateCodes, countyFilters) {
  const stateCodes = Array.isArray(selectedStateCodes) ? selectedStateCodes : [];
  const counties = Array.isArray(countyFilters) ? countyFilters : [];
  if (stateCodes.length === 0 && counties.length === 0) return true;

  const location = String(locationText || "").trim();
  if (!location) return false;
  const normalizedLocation = normalizeLikeText(location);

  if (stateCodes.length > 0) {
    const hasSelectedState = stateCodes.some((stateCode) => hasStateLikeMatch(location, stateCode));
    if (!hasSelectedState) return false;
  }

  if (counties.length > 0) {
    const matchesCounty = counties.some((countyFilter) => {
      const countyLikePart = String(countyFilter?.countyLikePart || "").trim();
      if (!countyLikePart) return false;

      if (countyFilter.stateCode && !hasStateLikeMatch(location, countyFilter.stateCode)) {
        return false;
      }

      return (
        normalizedLocation.includes(countyLikePart) ||
        normalizedLocation.includes(`${countyLikePart} county`) ||
        normalizedLocation.includes(`${countyLikePart} parish`) ||
        normalizedLocation.includes(`${countyLikePart} borough`) ||
        normalizedLocation.includes(`${countyLikePart} census area`)
      );
    });

    if (!matchesCounty) return false;
  }

  return true;
}

function rowMatchesRemoteFilter(locationText, remoteFilter) {
  const normalized = normalizeRemoteFilter(remoteFilter);
  if (!normalized || normalized === "all") return true;
  const isRemote = isRemoteLocation(locationText);
  if (normalized === "remote") return isRemote;
  if (normalized === "non_remote") return !isRemote;
  return true;
}

function normalizeRemoteFilter(value) {
  const normalized = String(value || "all")
    .trim()
    .toLowerCase();
  if (normalized === "remote" || normalized === "non_remote") return normalized;
  return "all";
}

function inferAtsFromJobPostingUrl(value) {
  const url = String(value || "").trim().toLowerCase();
  if (!url) return "";
  if (url.includes("myworkdayjobs.com")) return "workday";
  if (url.includes("jobs.ashbyhq.com")) return "ashby";
  if (url.includes("job-boards.greenhouse.io") || url.includes("boards.greenhouse.io")) return "greenhouse";
  if (url.includes("jobs.lever.co")) return "lever";
  if (url.includes(".recruitee.com")) return "recruitee";
  if (url.includes("recruiting.ultipro.com/") && url.includes("/jobboard/")) return "ultipro";
  if (url.includes(".taleo.net/careersection/")) return "taleo";
  if ((url.includes("jobs.jobvite.com/") || url.includes("careers.jobvite.com/")) && url.includes("/job/")) {
    return "jobvite";
  }
  if (url.includes(".applicantpro.com/jobs")) return "applicantpro";
  if (url.includes(".applytojob.com/apply")) return "applytojob";
  if (url.includes(".icims.com/jobs/")) return "icims";
  if (url.includes("theapplicantmanager.com/jobs")) return "theapplicantmanager";
  if (url.includes(".breezy.hr/p/")) return "breezy";
  if (url.includes(".zohorecruit.com/jobs/careers")) return "zoho";
  if (url.includes("applicantai.com/")) return "applicantai";
  if (url.includes(".bamboohr.com/careers")) return "bamboohr";
  if (url.includes("app.careerpuck.com/job-board/")) return "careerpuck";
  if (url.includes("web.fountain.com/c/")) return "fountain";
  if (url.includes(".getro.com/jobs")) return "getro";
  if (url.includes(".hrmdirect.com/employment/job-opening.php")) return "hrmdirect";
  if (url.includes(".talentlyft.com/jobs/")) return "talentlyft";
  if (url.includes(".talexio.com/jobs")) return "talexio";
  if (url.includes(".teamtailor.com/jobs/")) return "teamtailor";
  if (url.endsWith(".teamtailor.com/jobs")) return "teamtailor";
  if (url.includes(".careerplug.com/jobs/")) return "careerplug";
  if (url.endsWith(".careerplug.com/jobs")) return "careerplug";
  if (url.includes("jobs.gem.com/")) return "gem";
  if (url.includes(".jobapscloud.com")) return "jobaps";
  if (url.includes("join.com/companies/")) return "join";
  if (url.includes("apply.jobappnetwork.com/apply/")) return "talentreef";
  if (url.includes(".jobs.hr.cloud.sap/job/")) return "saphrcloud";
  if (url.includes(".jobs.hr.cloud.sap/search/")) return "saphrcloud";
  if (url.includes(".careers-page.com/jobs/")) return "manatal";
  if (url.includes(".careers-page.com/job/")) return "manatal";
  if (url.includes("www.careers-page.com/") && (url.includes("/job/") || url.includes("/jobs/"))) {
    return "manatal";
  }
  return "";
}

function normalizeAtsFilterValue(value) {
  const normalized = normalizeLikeText(value);
  if (normalized === "ashbyhq") return "ashby";
  if (normalized === "greenhouseio" || normalized === "greenhouse.io") return "greenhouse";
  if (normalized === "leverco" || normalized === "lever.co") return "lever";
  if (normalized === "recruiteecom" || normalized === "recruitee.com") return "recruitee";
  if (normalized === "ukg") return "ultipro";
  if (normalized === "taleonet" || normalized === "taleo.net") return "taleo";
  if (normalized === "jobvitecom" || normalized === "jobvite.com") return "jobvite";
  if (normalized === "applicantprocom" || normalized === "applicantpro.com") return "applicantpro";
  if (normalized === "applytojobcom" || normalized === "applytojob.com") return "applytojob";
  if (normalized === "icimscom" || normalized === "icims.com") return "icims";
  if (normalized === "theapplicantmanagercom" || normalized === "theapplicantmanager.com") {
    return "theapplicantmanager";
  }
  if (normalized === "breezyhr" || normalized === "breezy.hr" || normalized === "breezyhrcom") {
    return "breezy";
  }
  if (normalized === "zohorecruit" || normalized === "zohorecruit.com" || normalized === "zohorecruitcom") {
    return "zoho";
  }
  if (normalized === "applicantai.com" || normalized === "applicantaicom") {
    return "applicantai";
  }
  if (normalized === "bamboohr.com" || normalized === "bamboohrcom") {
    return "bamboohr";
  }
  if (normalized === "careerplug.com" || normalized === "careerplugcom") {
    return "careerplug";
  }
  if (
    normalized === "manatal.com" ||
    normalized === "manatalcom" ||
    normalized === "careers-page.com" ||
    normalized === "careerspagecom"
  ) {
    return "manatal";
  }
  if (normalized === "careerpuck.com" || normalized === "careerpuckcom") {
    return "careerpuck";
  }
  if (normalized === "fountain.com" || normalized === "fountaincom") {
    return "fountain";
  }
  if (normalized === "getro.com" || normalized === "getrocom") {
    return "getro";
  }
  if (normalized === "hrmdirect.com" || normalized === "hrmdirectcom") {
    return "hrmdirect";
  }
  if (normalized === "talentlyft.com" || normalized === "talentlyftcom") {
    return "talentlyft";
  }
  if (normalized === "talexio.com" || normalized === "talexiocom") {
    return "talexio";
  }
  if (normalized === "teamtailor.com" || normalized === "teamtailorcom") {
    return "teamtailor";
  }
  if (normalized === "jobs.gem.com" || normalized === "gem.com" || normalized === "gemcom") {
    return "gem";
  }
  if (normalized === "jobapscloud.com" || normalized === "jobapscloudcom") {
    return "jobaps";
  }
  if (normalized === "join.com" || normalized === "joincom") {
    return "join";
  }
  if (
    normalized === "jobappnetwork.com" ||
    normalized === "jobappnetworkcom" ||
    normalized === "apply.jobappnetwork.com" ||
    normalized === "applyjobappnetworkcom"
  ) {
    return "talentreef";
  }
  if (
    normalized === "saphrcloud" ||
    normalized === "saphrcloud.com" ||
    normalized === "saphrcloudcom" ||
    normalized === "jobs.hr.cloud.sap" ||
    normalized === "jobshrcloudsap"
  ) {
    return "saphrcloud";
  }
  return normalized;
}

function normalizeAtsFilters(value) {
  const items = normalizeStringArray(Array.isArray(value) ? value : [value])
    .map((item) => normalizeAtsFilterValue(item))
    .filter((item) => ATS_FILTER_OPTIONS.has(item));
  return Array.from(new Set(items));
}

function normalizePostingSort(value) {
  const normalized = normalizeLikeText(value);
  if (normalized === "company_asc" || normalized === "alphabetical") {
    return "company_asc";
  }
  if (POSTING_SORT_OPTIONS.has(normalized)) {
    return normalized;
  }
  return "recent";
}

function getPostingsOrderByClause(sortBy) {
  if (sortBy === "company_asc") {
    return "company_name ASC, position_name ASC";
  }
  return "COALESCE(last_seen_epoch, 0) DESC, id DESC";
}

function shuffleArrayInPlace(values) {
  const items = Array.isArray(values) ? values : [];
  for (let i = items.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [items[i], items[j]] = [items[j], items[i]];
  }
  return items;
}

function normalizeStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function normalizeApplicationStatus(value) {
  const normalized = normalizeLikeText(value);
  if (APPLICATION_STATUS_OPTIONS.has(normalized)) {
    return normalized;
  }
  return "applied";
}

function normalizeAppliedByType(value) {
  const normalized = normalizeLikeText(value);
  if (normalized === "ai" || normalized === "agent") return normalized;
  return "manual";
}

function normalizeAppliedByLabel(value, appliedByType = "manual") {
  const explicit = String(value || "").trim();
  if (explicit) return explicit;
  if (appliedByType === "ai" || appliedByType === "agent") {
    return "AI agent applied on behalf of user";
  }
  return "Manually applied by user";
}

function normalizeIgnoredByLabel(value) {
  const explicit = String(value || "").trim();
  if (explicit) return explicit;
  return "Ignored by user";
}

function normalizeBoolean(value, defaultValue = false) {
  if (typeof value === "boolean") return value;
  const normalized = normalizeLikeText(value);
  if (!normalized) return Boolean(defaultValue);
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function normalizeMcpRemotePreference(value) {
  const normalized = normalizeLikeText(value);
  if (MCP_REMOTE_OPTIONS.has(normalized)) return normalized;
  return "all";
}

function normalizeMcpSettingsInput(value = {}) {
  const source = value && typeof value === "object" ? value : {};
  const agentLoginEmail = String(source.agent_login_email ?? MCP_SETTINGS_DEFAULTS.agent_login_email).trim();

  return {
    enabled: normalizeBoolean(source.enabled, MCP_SETTINGS_DEFAULTS.enabled),
    preferred_agent_name: String(source.preferred_agent_name ?? MCP_SETTINGS_DEFAULTS.preferred_agent_name).trim() ||
      MCP_SETTINGS_DEFAULTS.preferred_agent_name,
    agent_login_email: agentLoginEmail,
    agent_login_password: String(source.agent_login_password ?? MCP_SETTINGS_DEFAULTS.agent_login_password),
    mfa_login_email: agentLoginEmail,
    mfa_login_notes: String(source.mfa_login_notes ?? MCP_SETTINGS_DEFAULTS.mfa_login_notes).trim(),
    dry_run_only: normalizeBoolean(source.dry_run_only, MCP_SETTINGS_DEFAULTS.dry_run_only),
    require_final_approval: normalizeBoolean(
      source.require_final_approval,
      MCP_SETTINGS_DEFAULTS.require_final_approval
    ),
    max_applications_per_run:
      parseNonNegativeInteger(source.max_applications_per_run) || MCP_SETTINGS_DEFAULTS.max_applications_per_run,
    preferred_search: String(source.preferred_search ?? MCP_SETTINGS_DEFAULTS.preferred_search).trim(),
    preferred_remote: normalizeMcpRemotePreference(source.preferred_remote),
    preferred_industries: parseJsonArray(source.preferred_industries),
    preferred_states: parseJsonArray(source.preferred_states).map((state) => state.toUpperCase()),
    preferred_counties: parseJsonArray(source.preferred_counties),
    instructions_for_agent: String(source.instructions_for_agent ?? MCP_SETTINGS_DEFAULTS.instructions_for_agent).trim()
  };
}

function ensureMcpAgentEnabled(settings) {
  if (normalizeBoolean(settings?.enabled, false)) return;
  const error = new Error("MCP application agent is disabled in settings.");
  error.statusCode = 403;
  throw error;
}

function createDefaultPersonalInformation() {
  return { ...PERSONAL_INFORMATION_DEFAULTS };
}

function parseNonNegativeInteger(value) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function parsePositiveInteger(value) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function normalizeAtsRequestQueueConcurrency(value, fallbackValue = ATS_REQUEST_QUEUE_CONCURRENCY_DEFAULT) {
  const fallback = parsePositiveInteger(fallbackValue) || ATS_REQUEST_QUEUE_CONCURRENCY_DEFAULT;
  const parsed = parsePositiveInteger(value) || fallback;
  return Math.max(MIN_ATS_REQUEST_QUEUE_CONCURRENCY, Math.min(MAX_ATS_REQUEST_QUEUE_CONCURRENCY, parsed));
}

function normalizeSyncServiceSettingsInput(value = {}, fallback = SYNC_SERVICE_SETTINGS_DEFAULTS) {
  const source = value && typeof value === "object" ? value : {};
  const fallbackConcurrency = normalizeAtsRequestQueueConcurrency(fallback?.ats_request_queue_concurrency);
  return {
    ats_request_queue_concurrency: normalizeAtsRequestQueueConcurrency(
      source.ats_request_queue_concurrency,
      fallbackConcurrency
    )
  };
}

function normalizePersonalInformationInput(value) {
  const source = value && typeof value === "object" ? value : {};
  const normalized = createDefaultPersonalInformation();
  const numericFields = new Set(["age", "years_of_experience"]);
  const textFields = PERSONAL_INFORMATION_FIELDS.filter((field) => !numericFields.has(field));

  for (const field of textFields) {
    normalized[field] = String(source[field] ?? "").trim();
  }

  normalized.age = parseNonNegativeInteger(source.age);
  normalized.years_of_experience = parseNonNegativeInteger(source.years_of_experience);

  return normalized;
}

function parseUrl(urlString) {
  if (!urlString) return null;
  try {
    return new URL(urlString);
  } catch {
    return null;
  }
}

function pickCompanyId(pathParts, subdomain) {
  if (!Array.isArray(pathParts) || pathParts.length === 0) return subdomain;

  const [first = "", second = ""] = pathParts;
  if (first && LOCALE_SEGMENT_REGEX.test(first) && second) {
    return second;
  }

  return first || subdomain;
}

function parseWorkdayCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const [subdomain = ""] = parsed.hostname.split(".");
  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  const companyIdRaw = pickCompanyId(pathParts, subdomain);
  const companyIdApi = companyIdRaw.toLowerCase();

  if (!subdomain || !companyIdApi) return null;

  return {
    subdomain: subdomain.toLowerCase(),
    companyIdRaw,
    companyIdApi,
    companyBaseUrl: `${parsed.origin}/${companyIdRaw}`,
    cxsUrl: `${parsed.origin}/wday/cxs/${subdomain.toLowerCase()}/${companyIdApi}/jobs`
  };
}

function isPostedToday(postedOn) {
  if (typeof postedOn !== "string") return false;
  return postedOn.trim().toLowerCase() === "posted today";
}

function parsePostingDateToEpochSeconds(postingDate, referenceEpoch = nowEpochSeconds()) {
  const raw = String(postingDate ?? "").trim();
  if (!raw) return null;

  const normalizedLower = raw.toLowerCase();
  if (normalizedLower === "posted today" || normalizedLower === "today") {
    return Number(referenceEpoch);
  }
  if (normalizedLower === "posted yesterday" || normalizedLower === "yesterday") {
    return Number(referenceEpoch) - 24 * 60 * 60;
  }

  const daysAgoMatch = normalizedLower.match(/^(\d+)\s+day(?:s)?\s+ago$/i);
  if (daysAgoMatch?.[1]) {
    return Number(referenceEpoch) - Number(daysAgoMatch[1]) * 24 * 60 * 60;
  }

  const hoursAgoMatch = normalizedLower.match(/^(\d+)\s+hour(?:s)?\s+ago$/i);
  if (hoursAgoMatch?.[1]) {
    return Number(referenceEpoch) - Number(hoursAgoMatch[1]) * 60 * 60;
  }

  let normalized = raw
    .replace(/^posted\s+/i, "")
    .replace(/\b(\d{1,2})(st|nd|rd|th)\b/gi, "$1")
    .replace(/\s+/g, " ")
    .trim();

  if (/^\d{10,13}$/.test(normalized)) {
    const numericEpoch = Number(normalized.length === 13 ? Math.floor(Number(normalized) / 1000) : normalized);
    if (Number.isFinite(numericEpoch) && numericEpoch > 0) {
      return numericEpoch;
    }
  }

  const parsedMs = Date.parse(normalized);
  if (Number.isFinite(parsedMs)) return Math.floor(parsedMs / 1000);

  normalized = normalized.replace(/,\s*/g, " ").trim();
  const fallbackParsedMs = Date.parse(normalized);
  if (Number.isFinite(fallbackParsedMs)) return Math.floor(fallbackParsedMs / 1000);

  return null;
}

function shouldStorePostingByDate(postingDate, referenceEpoch = nowEpochSeconds()) {
  const raw = String(postingDate ?? "").trim();
  if (!raw) return true;

  const parsedEpoch = parsePostingDateToEpochSeconds(raw, referenceEpoch);
  if (!parsedEpoch) return false;
  return parsedEpoch >= Number(referenceEpoch) - POSTING_TTL_SECONDS;
}

function buildJobUrl(companyBaseUrl, externalPath) {
  if (typeof externalPath !== "string" || !externalPath.trim()) return "";
  const normalizedPath = externalPath.startsWith("/") ? externalPath : `/${externalPath}`;
  return `${companyBaseUrl}${normalizedPath}`;
}

function formatLocationSegment(rawLocation) {
  if (typeof rawLocation !== "string") return null;
  const trimmed = rawLocation.trim();
  if (!trimmed) return null;

  const doubleDashToken = "__DOUBLE_DASH__";
  return trimmed
    .replace(/--+/g, doubleDashToken)
    .replace(/-/g, " ")
    .replace(new RegExp(doubleDashToken, "g"), "- ")
    .replace(/\s+/g, " ")
    .trim();
}

function inferWorkdayLocationFromJobUrl(jobPostingUrl) {
  try {
    const parsed = new URL(String(jobPostingUrl || ""));
    const pathParts = parsed.pathname
      .split("/")
      .map((part) => String(part || "").trim())
      .filter(Boolean);
    const jobIndex = pathParts.findIndex((part) => part.toLowerCase() === "job");
    if (jobIndex >= 0 && pathParts[jobIndex + 1] && pathParts[jobIndex + 2]) {
      const rawLocation = decodeURIComponent(pathParts[jobIndex + 1]);
      return formatLocationSegment(rawLocation);
    }
    return null;
  } catch {
    return null;
  }
}

function inferPostingLocationFromJobUrl(jobPostingUrl) {
  const url = String(jobPostingUrl || "").trim();
  if (!url) return null;

  try {
    const parsed = new URL(url);
    if (parsed.hostname.endsWith("myworkdayjobs.com")) {
      return inferWorkdayLocationFromJobUrl(url);
    }
    if (parsed.hostname === "jobs.ashbyhq.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "job-boards.greenhouse.io" || parsed.hostname === "boards.greenhouse.io") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "jobs.lever.co") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".recruitee.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "recruiting.ultipro.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".taleo.net")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "jobs.jobvite.com" || parsed.hostname === "careers.jobvite.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".applicantpro.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".applytojob.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".icims.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith("theapplicantmanager.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".breezy.hr")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".zohorecruit.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".bamboohr.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "applicantai.com" || parsed.hostname === "www.applicantai.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".careerplug.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "www.careers-page.com" || parsed.hostname.endsWith(".careers-page.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "app.careerpuck.com" || parsed.hostname === "www.app.careerpuck.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "web.fountain.com" || parsed.hostname === "www.web.fountain.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".getro.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".hrmdirect.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".talentlyft.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".talexio.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".teamtailor.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "jobs.gem.com" || parsed.hostname === "www.jobs.gem.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".jobapscloud.com")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "join.com" || parsed.hostname === "www.join.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname === "apply.jobappnetwork.com" || parsed.hostname === "www.apply.jobappnetwork.com") {
      return postingLocationByJobUrl.get(url) || null;
    }
    if (parsed.hostname.endsWith(".jobs.hr.cloud.sap")) {
      return postingLocationByJobUrl.get(url) || null;
    }
    return null;
  } catch {
    return null;
  }
}

function extractAshbyLocationName(posting) {
  const names = [];
  const primary = String(posting?.locationName || "").trim();
  if (primary) names.push(primary);

  const secondary = Array.isArray(posting?.secondaryLocations) ? posting.secondaryLocations : [];
  for (const location of secondary) {
    const name = String(location?.locationName || "").trim();
    if (!name) continue;
    if (names.some((existing) => existing.toLowerCase() === name.toLowerCase())) continue;
    names.push(name);
  }

  return names.length > 0 ? names.join(", ") : null;
}

function parseAshbyCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  const [organizationHostedJobsPageName = ""] = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (!organizationHostedJobsPageName) return null;

  return {
    organizationHostedJobsPageName,
    organizationHostedJobsPageNameLower: organizationHostedJobsPageName.toLowerCase()
  };
}

function parseGreenhouseCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  const [boardToken = ""] = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (!boardToken) return null;

  return {
    boardToken,
    boardTokenLower: boardToken.toLowerCase()
  };
}

function parseLeverCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  const [organization = ""] = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (!organization) return null;

  return {
    organization,
    organizationLower: organization.toLowerCase()
  };
}

function parseJobviteCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "jobs.jobvite.com" && host !== "careers.jobvite.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length === 0) return null;

  const companySlug = String(pathParts[0] || "").trim();
  if (!companySlug) return null;

  return {
    host,
    companySlug,
    companySlugLower: companySlug.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    jobsUrl: `${parsed.protocol}//${parsed.host}/${companySlug}/jobs`
  };
}

function parseCareerplugCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".careerplug.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    jobsUrl: `${parsed.protocol}//${parsed.host}/jobs`
  };
}

function parseBambooHrCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  const suffix = ".bamboohr.com";
  if (!host.endsWith(suffix)) return null;

  const companySubdomain = String(host.slice(0, -suffix.length) || "").trim();
  if (!companySubdomain || companySubdomain.includes(".") || companySubdomain === "www") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length > 0 && String(pathParts[0] || "").toLowerCase() !== "careers") return null;

  const baseOrigin = `${parsed.protocol}//${parsed.host}`;
  return {
    host,
    companySubdomain,
    companySubdomainLower: companySubdomain.toLowerCase(),
    baseOrigin,
    boardUrl: `${baseOrigin}/careers`,
    apiUrl: `${baseOrigin}/careers/list`
  };
}

function parseCareerpuckCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "app.careerpuck.com" && host !== "www.app.careerpuck.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length < 2 || pathParts[0].toLowerCase() !== "job-board") return null;

  const boardSlug = String(pathParts[1] || "").trim();
  if (!boardSlug) return null;

  return {
    host,
    boardSlug,
    boardSlugLower: boardSlug.toLowerCase(),
    boardUrl: `${parsed.protocol}//${parsed.host}/job-board/${boardSlug}`,
    apiUrl: `https://api.careerpuck.com/v1/public/job-boards/${encodeURIComponent(boardSlug)}`
  };
}

function parseFountainCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "web.fountain.com" && host !== "www.web.fountain.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length < 4 || pathParts[0].toLowerCase() !== "c") return null;

  const companyPath = pathParts.slice(0, 4);
  const companySlug = String(pathParts[1] || "").trim();
  if (!companySlug) return null;

  const boardPath = companyPath.join("/");
  const boardUrl = `${parsed.protocol}//${parsed.host}/${boardPath}`;

  return {
    host,
    companySlug,
    companySlugLower: companySlug.toLowerCase(),
    boardUrl,
    apiUrl: `${boardUrl}.json`
  };
}

function parseGetroCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host === "www.getro.com") return null;
  if (!host.endsWith(".getro.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    jobsUrl: `${parsed.protocol}//${parsed.host}/jobs`
  };
}

function parseHrmDirectCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".hrmdirect.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  const jobsUrl = new URL(parsed.toString());
  if (!/\/employment\/job-openings\.php$/i.test(String(jobsUrl.pathname || ""))) {
    jobsUrl.pathname = "/employment/job-openings.php";
  }
  if (!jobsUrl.searchParams.has("search")) {
    jobsUrl.searchParams.set("search", "true");
  }
  jobsUrl.hash = "";

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    jobsUrl: jobsUrl.toString()
  };
}

function parseTalentlyftCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".talentlyft.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    careersUrl: `${parsed.protocol}//${parsed.host}/`
  };
}

function parseTalexioCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".talexio.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length > 0 && String(pathParts[0] || "").toLowerCase() !== "jobs") return null;

  const baseOrigin = `${parsed.protocol}//${parsed.host}`;
  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    baseOrigin,
    jobsUrl: `${baseOrigin}/jobs/`,
    apiUrl: `${baseOrigin}/api/jobs`
  };
}

function parseSapHrCloudCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  const suffix = ".jobs.hr.cloud.sap";
  if (!host.endsWith(suffix)) return null;

  const companyName = String(host.slice(0, -suffix.length) || "").trim();
  if (!companyName) return null;

  const localeFromUrl = String(parsed.searchParams.get("locale") || "").trim();
  const baseOrigin = `${parsed.protocol}//${parsed.host}`;
  return {
    host,
    companyName,
    companyNameLower: companyName.toLowerCase(),
    baseOrigin,
    boardUrl: `${baseOrigin}/search/?createNewAlert=false&q=`,
    apiUrl: `${baseOrigin}/services/recruiting/v1/jobs`,
    localeFromUrl: localeFromUrl || ""
  };
}

function parseTeamtailorCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".teamtailor.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  const baseOrigin = `${parsed.protocol}//${parsed.host}`;
  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    baseOrigin,
    jobsUrl: `${baseOrigin}/jobs`
  };
}

function parseManatalCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "www.careers-page.com" && !host.endsWith(".careers-page.com")) return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);

  const hostSubdomain =
    host.endsWith(".careers-page.com") && host !== "www.careers-page.com"
      ? String(host.split(".")[0] || "").trim()
      : "";

  let domainSlug = hostSubdomain || String(pathParts[0] || "").trim();
  if (!domainSlug) return null;
  domainSlug = domainSlug.toLowerCase();
  if (!domainSlug || domainSlug === "job" || domainSlug === "jobs") return null;

  const baseOrigin = `${parsed.protocol}//${parsed.host}`;
  const publicBaseUrl = "https://www.careers-page.com";
  const boardUrl =
    host === "www.careers-page.com" ? `${baseOrigin}/${domainSlug}/` : `${baseOrigin}/`;

  return {
    host,
    domainSlug,
    domainSlugLower: domainSlug.toLowerCase(),
    baseOrigin,
    publicBaseUrl,
    boardUrl,
    careersUrl: boardUrl,
    jobsApiUrl: `${publicBaseUrl}/api/v1.0/c/${encodeURIComponent(domainSlug)}/jobs/`
  };
}

function parseGemCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "jobs.gem.com" && host !== "www.jobs.gem.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length === 0) return null;

  const boardId = String(pathParts[0] || "").trim();
  if (!boardId) return null;

  return {
    host,
    boardId,
    boardIdLower: boardId.toLowerCase(),
    boardUrl: `${parsed.protocol}//${parsed.host}/${boardId}`,
    apiUrl: "https://jobs.gem.com/api/public/graphql/batch"
  };
}

function parseJobApsCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".jobapscloud.com")) return null;

  const boardUrl = parsed.toString();
  return {
    host,
    boardUrl
  };
}

function parseJoinCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "join.com" && host !== "www.join.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length < 2 || String(pathParts[0] || "").toLowerCase() !== "companies") return null;

  const companySlug = String(pathParts[1] || "").trim();
  if (!companySlug) return null;

  return {
    host,
    companySlug,
    companySlugLower: companySlug.toLowerCase(),
    boardUrl: `${parsed.protocol}//${parsed.host}/companies/${companySlug}`
  };
}

function parseTalentreefCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "apply.jobappnetwork.com" && host !== "www.apply.jobappnetwork.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length === 0) return null;

  const companyName = String(pathParts[0] || "").trim();
  if (!companyName) return null;

  return {
    host,
    companyName,
    companyNameLower: companyName.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    boardUrl: `${parsed.protocol}//${parsed.host}/${companyName}`,
    aliasApiUrl: `https://prod-kong.internal.talentreef.com/apply/careerPages/alias/${encodeURIComponent(companyName)}`,
    searchApiUrl: "https://prod-kong.internal.talentreef.com/apply/proxy-es/search-en-us/posting/_search"
  };
}

function parseApplicantProCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".applicantpro.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  const jobsUrl = `${parsed.protocol}//${parsed.host}/jobs/`;
  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    origin: `${parsed.protocol}//${parsed.host}`,
    jobsUrl
  };
}

function parseApplyToJobCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".applytojob.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    applyUrl: `${parsed.protocol}//${parsed.host}/apply`
  };
}

function parseTheApplicantManagerCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "theapplicantmanager.com" && host !== "www.theapplicantmanager.com") return null;

  const companyCode = String(parsed.searchParams.get("co") || "").trim().toLowerCase();
  if (!companyCode) return null;

  return {
    host,
    companyCode,
    companyCodeLower: companyCode.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    careersUrl: `${parsed.protocol}//${parsed.host}/careers?co=${encodeURIComponent(companyCode)}`
  };
}

function parseIcimsCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".icims.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  const searchUrl = new URL(parsed.toString());
  searchUrl.pathname = "/jobs/search";
  if (!searchUrl.searchParams.has("ss")) {
    searchUrl.searchParams.set("ss", "1");
  }
  searchUrl.searchParams.delete("in_iframe");

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    origin: `${parsed.protocol}//${parsed.host}`,
    searchUrl: searchUrl.toString()
  };
}

function parseBreezyCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host === "breezy.hr" || host === "www.breezy.hr") return null;
  if (!host.endsWith(".breezy.hr")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    origin: `${parsed.protocol}//${parsed.host}`,
    portalUrl: `${parsed.protocol}//${parsed.host}/`
  };
}

function parseZohoCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".zohorecruit.com")) return null;

  const [subdomain = ""] = host.split(".");
  if (!subdomain) return null;

  const careersUrl = new URL(parsed.toString());
  careersUrl.pathname = "/jobs/Careers";
  careersUrl.search = "";
  careersUrl.hash = "";

  return {
    host,
    subdomain,
    subdomainLower: subdomain.toLowerCase(),
    origin: `${parsed.protocol}//${parsed.host}`,
    careersUrl: careersUrl.toString()
  };
}

function parseApplicantAiCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "applicantai.com" && host !== "www.applicantai.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  const slug = String(pathParts[0] || "").trim();
  if (!slug) return null;

  return {
    host,
    slug,
    slugLower: slug.toLowerCase(),
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    careersUrl: `${parsed.protocol}//${parsed.host}/${slug}`
  };
}

function parseRecruiteeCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;
  if (!String(parsed.hostname || "").toLowerCase().endsWith(".recruitee.com")) return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  const normalizedPathParts = pathParts[0]?.toLowerCase() === "o" ? [] : pathParts;
  const basePath = normalizedPathParts.length > 0 ? `/${normalizedPathParts.join("/")}` : "";
  const baseUrl = `${parsed.origin}${basePath}`.replace(/\/+$/, "");
  const [subdomain = ""] = parsed.hostname.split(".");

  return {
    baseUrl: baseUrl || parsed.origin,
    subdomain: String(subdomain || "").toLowerCase()
  };
}

function parseUltiProCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (host !== "recruiting.ultipro.com") return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);

  const jobBoardIndex = pathParts.findIndex((part) => part.toLowerCase() === "jobboard");
  if (jobBoardIndex <= 0 || jobBoardIndex + 1 >= pathParts.length) return null;

  const tenant = pathParts[jobBoardIndex - 1];
  const boardId = pathParts[jobBoardIndex + 1];
  if (!tenant || !boardId) return null;

  return {
    tenant,
    tenantLower: tenant.toLowerCase(),
    boardId,
    baseBoardUrl: `${parsed.protocol}//${parsed.host}/${tenant}/JobBoard/${boardId}`
  };
}

function parseTaleoCompany(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return null;

  const host = String(parsed.hostname || "").toLowerCase();
  if (!host.endsWith(".taleo.net")) return null;

  const pathParts = parsed.pathname
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);

  if (pathParts.length < 2 || pathParts[0].toLowerCase() !== "careersection") return null;

  const careerSection = pathParts[1];
  if (!careerSection) return null;

  const lang = String(parsed.searchParams.get("lang") || "en").trim() || "en";

  return {
    careerSection,
    careerSectionLower: careerSection.toLowerCase(),
    lang,
    baseOrigin: `${parsed.protocol}//${parsed.host}`,
    baseSectionUrl: `${parsed.protocol}//${parsed.host}/careersection/${careerSection}`
  };
}

function extractTaleoRestConfig(pageHtml) {
  const source = String(pageHtml || "");
  const portalMatch = source.match(/portal=([0-9]{6,})/i);
  const portal = String(portalMatch?.[1] || "").trim();

  const tokenNamePatterns = [
    /sessionCSRFTokenName\s*:\s*'([^']+)'/i,
    /sessionCSRFTokenName\s*:\s*"([^"]+)"/i,
    /"sessionCSRFTokenName"\s*:\s*"([^"]+)"/i,
    /name=['"](csrftoken)['"]/i
  ];
  const tokenValuePatterns = [
    /sessionCSRFToken\s*:\s*'([^']+)'/i,
    /sessionCSRFToken\s*:\s*"([^"]+)"/i,
    /"sessionCSRFToken"\s*:\s*"([^"]+)"/i,
    /name=["']csrftoken["'][^>]*value=["']([^"']+)["']/i
  ];

  let tokenName = "";
  let tokenValue = "";

  for (const pattern of tokenNamePatterns) {
    const match = source.match(pattern);
    if (!match?.[1]) continue;
    tokenName = String(match[1] || "").trim();
    if (tokenName) break;
  }

  for (const pattern of tokenValuePatterns) {
    const match = source.match(pattern);
    if (!match?.[1]) continue;
    tokenValue = String(match[1] || "").trim();
    if (tokenValue) break;
  }

  return { portal, tokenName, tokenValue };
}

function extractApplicantProDomainId(pageHtml) {
  const source = String(pageHtml || "");
  const patterns = [
    /["']domain_id["']\s*:\s*["']?(\d{2,})["']?/i,
    /domain_id\s*=\s*["']?(\d{2,})["']?/i
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    const value = String(match?.[1] || "").trim();
    if (value) return value;
  }

  return "";
}

function buildTaleoRestPayload(pageNo = 1) {
  return {
    multilineEnabled: true,
    sortingSelection: {
      sortBySelectionParam: "1",
      ascendingSortingOrder: "false"
    },
    fieldData: {
      fields: {
        LOCATION: "",
        CATEGORY: "",
        KEYWORD: ""
      },
      valid: true
    },
    filterSelectionParam: {
      searchFilterSelections: [
        { id: "JOB_FIELD", selectedValues: [] },
        { id: "LOCATION", selectedValues: [] },
        { id: "ORGANIZATION", selectedValues: [] },
        { id: "JOB_LEVEL", selectedValues: [] }
      ]
    },
    advancedSearchFiltersSelectionParam: {
      searchFilterSelections: [
        { id: "ORGANIZATION", selectedValues: [] },
        { id: "LOCATION", selectedValues: [] },
        { id: "JOB_FIELD", selectedValues: [] },
        { id: "JOB_NUMBER", selectedValues: [] },
        { id: "URGENT_JOB", selectedValues: [] },
        { id: "JOB_SHIFT", selectedValues: [] }
      ]
    },
    pageNo: Number(pageNo || 1)
  };
}

function buildTaleoAjaxPayload(lang = "en", csrfToken = "") {
  const payload = {
    ftlpageid: "reqListBasicPage",
    ftlinterfaceid: "requisitionListInterface",
    ftlcompid: "validateTimeZoneId",
    jsfCmdId: "validateTimeZoneId",
    ftlcompclass: "InitTimeZoneAction",
    ftlcallback: "requisition_restoreDatesValues",
    ftlajaxid: "ftlx1",
    tz: "GMT-07:00",
    tzname: "America/Los_Angeles",
    lang: String(lang || "en").trim() || "en",
    isExternal: "true",
    "rlPager.currentPage": "1",
    "listRequisition.size": "25",
    dropListSize: "25"
  };

  if (csrfToken) {
    payload.csrftoken = String(csrfToken || "").trim();
  }

  return payload;
}

function extractTaleoLocationLabel(value) {
  const text = String(value || "").trim();
  if (!text) return null;

  if (text.startsWith("[") && text.endsWith("]")) {
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        const normalized = parsed.map((item) => String(item || "").trim()).filter(Boolean);
        if (normalized.length > 0) return normalized.join(" / ");
      }
    } catch {
      // Fall through to the raw string value.
    }
  }

  return text;
}

function extractTaleoPostingsFromRest(companyNameForPostings, config, requisitions) {
  const items = Array.isArray(requisitions) ? requisitions : [];
  const postings = [];

  for (const requisition of items) {
    const jobId = String(requisition?.jobId || requisition?.contestNo || "").trim();
    if (!jobId) continue;

    const columns = Array.isArray(requisition?.column) ? requisition.column : [];
    const title = String(columns[0] || "").trim() || "Untitled Position";
    const location = extractTaleoLocationLabel(columns[2] || "");
    const postingDate = String(columns[4] || "").trim() || null;
    const contestNo = String(requisition?.contestNo || "").trim();
    const detailRef = contestNo || jobId;
    const jobUrl = detailRef
      ? `${config.baseSectionUrl}/jobdetail.ftl?job=${encodeURIComponent(detailRef)}&lang=${encodeURIComponent(
          config.lang
        )}`
      : `${config.baseSectionUrl}/jobsearch.ftl?lang=${encodeURIComponent(config.lang)}`;

    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location
    });
  }

  return postings;
}

function extractTaleoPostingsFromAjax(companyNameForPostings, config, ajaxText) {
  const source = String(ajaxText || "");
  if (!source.includes("!|!")) return [];

  const tokens = source.split("!|!");
  const applyPrefix = "Apply for this position (";
  const postings = [];
  const seenKeys = new Set();

  for (let index = 0; index < tokens.length; index += 1) {
    const tokenText = String(tokens[index] || "").trim();
    if (!tokenText.startsWith(applyPrefix)) continue;

    let titleFromApply = tokenText.slice(applyPrefix.length).trim();
    if (titleFromApply.endsWith(")")) {
      titleFromApply = titleFromApply.slice(0, -1).trim();
    }

    const postedDate = index >= 2 ? String(tokens[index - 2] || "").trim() : "";
    const locationRaw = index >= 8 ? String(tokens[index - 8] || "").trim() : "";
    const jobNumber = index >= 9 ? String(tokens[index - 9] || "").trim() : "";
    let jobId = index >= 14 ? String(tokens[index - 14] || "").trim() : "";
    const fallbackTitle = index >= 13 ? String(tokens[index - 13] || "").trim() : "";

    if (!/^\d+$/.test(jobId)) {
      for (let step = 1; step <= 20; step += 1) {
        const candidate = String(tokens[index - step] || "").trim();
        if (/^\d+$/.test(candidate)) {
          jobId = candidate;
          break;
        }
      }
    }

    const title = titleFromApply || fallbackTitle || "Untitled Position";
    const detailRef = jobNumber || jobId;
    const location = extractTaleoLocationLabel(locationRaw);
    const dedupeKey = `${detailRef}|${title}|${location || ""}`.toLowerCase();
    if (!detailRef || seenKeys.has(dedupeKey)) continue;

    seenKeys.add(dedupeKey);
    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: `${config.baseSectionUrl}/jobdetail.ftl?job=${encodeURIComponent(
        detailRef
      )}&lang=${encodeURIComponent(config.lang)}`,
      posting_date: postedDate || null,
      location
    });
  }

  return postings;
}

function decodeHtmlEntities(value) {
  return String(value || "")
    .replace(/&#(\d+);/g, (_match, codePoint) => String.fromCharCode(Number(codePoint)))
    .replace(/&#x([0-9a-f]+);/gi, (_match, codePoint) => String.fromCharCode(parseInt(codePoint, 16)))
    .replace(/&quot;/g, "\"")
    .replace(/&#34;/g, "\"")
    .replace(/&#x22;/gi, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/gi, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function cleanJobviteText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function parseJobvitePostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const tablePattern =
    /<h3[^>]*>([\s\S]*?)<\/h3>\s*<table[^>]*class=["'][^"']*\bjv-job-list\b[^"']*["'][^>]*>([\s\S]*?)<\/table>/gi;
  const rowPattern =
    /<tr[^>]*>[\s\S]*?<td[^>]*class=["'][^"']*\bjv-job-list-name\b[^"']*["'][^>]*>[\s\S]*?<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>[\s\S]*?<\/td>[\s\S]*?<td[^>]*class=["'][^"']*\bjv-job-list-location\b[^"']*["'][^>]*>([\s\S]*?)<\/td>[\s\S]*?<\/tr>/gi;

  const postings = [];
  const seenUrls = new Set();

  const pushRows = (rowsHtml, department = "") => {
    let rowMatch = rowPattern.exec(rowsHtml);
    while (rowMatch) {
      const href = String(rowMatch[1] || "").trim();
      const absoluteUrl = href ? new URL(href, `${config.baseOrigin}/`).toString() : "";
      if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
        rowMatch = rowPattern.exec(rowsHtml);
        continue;
      }

      postings.push({
        company_name: companyNameForPostings,
        position_name: cleanJobviteText(rowMatch[2]) || "Untitled Position",
        job_posting_url: absoluteUrl,
        posting_date: null,
        location: cleanJobviteText(rowMatch[3]) || null,
        department: cleanJobviteText(department) || null
      });
      seenUrls.add(absoluteUrl);
      rowMatch = rowPattern.exec(rowsHtml);
    }
    rowPattern.lastIndex = 0;
  };

  let tableMatch = tablePattern.exec(source);
  while (tableMatch) {
    pushRows(String(tableMatch[2] || ""), String(tableMatch[1] || ""));
    tableMatch = tablePattern.exec(source);
  }

  if (postings.length === 0) {
    pushRows(source, "");
  }

  return postings;
}

function cleanCareerplugText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function normalizeCareerplugMeta(value) {
  return cleanCareerplugText(value)
    .replace(/^\s*Location:\s*/i, "")
    .replace(/^\s*Full\s*\/\s*Part\s*Time:\s*/i, "")
    .trim();
}

function parseCareerplugPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();

  const rowPattern =
    /<a[^>]*\baria-label=["'][^"']*["'][^>]*\bhref=["'](\/jobs\/\d+[^"']*)["'][^>]*>([\s\S]*?)<\/a>/gi;
  const titlePattern = /<div[^>]*class=["'][^"']*\bjob-title\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i;
  const locationPattern = /<div[^>]*class=["'][^"']*\bjob-location\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i;
  const typePattern = /<div[^>]*class=["'][^"']*\bjob-type\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i;

  let rowMatch = rowPattern.exec(source);
  while (rowMatch) {
    const href = String(rowMatch[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.baseOrigin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      rowMatch = rowPattern.exec(source);
      continue;
    }

    const rowBody = String(rowMatch[2] || "");
    const title = cleanCareerplugText(rowBody.match(titlePattern)?.[1] || "");
    const location = normalizeCareerplugMeta(rowBody.match(locationPattern)?.[1] || "");
    const jobType = normalizeCareerplugMeta(rowBody.match(typePattern)?.[1] || "");

    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: null,
      location: location || null,
      employment_type: jobType || null
    });
    seenUrls.add(absoluteUrl);
    rowMatch = rowPattern.exec(source);
  }

  return postings;
}

function cleanTeamtailorText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function extractTeamtailorMetaParts(value) {
  const source = String(value || "");
  const parts = [];
  const seen = new Set();
  const spanPattern = /<span[^>]*>([\s\S]*?)<\/span>/gi;
  let spanMatch = spanPattern.exec(source);

  while (spanMatch) {
    const cleaned = cleanTeamtailorText(spanMatch[1] || "");
    const normalized = cleaned.toLowerCase();
    if (cleaned && cleaned !== "·" && cleaned !== "&middot;" && !seen.has(normalized)) {
      parts.push(cleaned);
      seen.add(normalized);
    }
    spanMatch = spanPattern.exec(source);
  }

  return parts;
}

function parseTeamtailorPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();
  const itemPattern =
    /<li[^>]*class=["'][^"']*\bblock-grid-item\b[^"']*["'][^>]*>([\s\S]*?)<\/li>/gi;
  const hrefPattern = /<a[^>]*href=["']([^"']+)["'][^>]*>/i;
  const titleAttrPattern =
    /<span[^>]*class=["'][^"']*\btext-block-base-link\b[^"']*["'][^>]*\btitle=["']([^"']+)["'][^>]*>/i;
  const titleBodyPattern =
    /<span[^>]*class=["'][^"']*\btext-block-base-link\b[^"']*["'][^>]*>([\s\S]*?)<\/span>/i;
  const metaPattern =
    /<div[^>]*class=["'][^"']*\bmt-1\b[^"']*\btext-md\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/i;

  let itemMatch = itemPattern.exec(source);
  while (itemMatch) {
    const itemHtml = String(itemMatch[1] || "");
    const hrefMatch = itemHtml.match(hrefPattern);
    const href = String(hrefMatch?.[1] || "").trim();
    const jobUrl = href ? new URL(href, `${config.baseOrigin || ""}/`).toString() : "";
    if (!jobUrl || seenUrls.has(jobUrl)) {
      itemMatch = itemPattern.exec(source);
      continue;
    }

    const titleFromAttr = cleanTeamtailorText(itemHtml.match(titleAttrPattern)?.[1] || "");
    const titleFromBody = cleanTeamtailorText(itemHtml.match(titleBodyPattern)?.[1] || "");
    const title = titleFromAttr || titleFromBody || "Untitled Position";

    const metaRaw = String(itemHtml.match(metaPattern)?.[1] || "");
    const metaParts = extractTeamtailorMetaParts(metaRaw);
    const department = metaParts.length > 1 ? metaParts[0] : null;
    const location = metaParts.length > 1 ? metaParts.slice(1).join(" / ") : metaParts[0] || null;

    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: jobUrl,
      posting_date: null,
      location,
      department
    });
    seenUrls.add(jobUrl);
    itemMatch = itemPattern.exec(source);
  }

  return postings;
}

function cleanManatalText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function extractManatalPageRuntimeConfig(pageHtml, fallbackConfig, finalUrl = "") {
  const source = String(pageHtml || "");
  const fallback = fallbackConfig && typeof fallbackConfig === "object" ? fallbackConfig : {};

  const baseUrlRaw = String(source.match(/const\s+baseUrl\s*=\s*['"]([^'"]+)['"]/i)?.[1] || "").trim();
  const publicBaseUrl = (baseUrlRaw || String(fallback.publicBaseUrl || "https://www.careers-page.com")).replace(
    /\/+$/,
    ""
  );

  const slugCandidates = [];
  const candidatePatterns = [
    /const\s+clientSlug\s*=\s*['"]([^'"]+)['"]/i,
    /data-domain_slug\s*=\s*['"]([^'"]+)['"]/i,
    /<a[^>]*class=['"][^'"]*\bnavbar-brand\b[^'"]*['"][^>]*href=['"]\/([^\/"'?#]+)/i,
    /<meta[^>]*property=['"]og:type['"][^>]*content=['"]\s*([^|'"]+?)\s*\|/i
  ];
  for (const pattern of candidatePatterns) {
    const value = String(source.match(pattern)?.[1] || "").trim();
    if (value) slugCandidates.push(value);
  }

  const finalParsed = parseUrl(finalUrl) || parseUrl(String(fallback.careersUrl || fallback.boardUrl || ""));
  const finalHost = String(finalParsed?.hostname || fallback.host || "").toLowerCase();
  if (finalHost.endsWith(".careers-page.com") && finalHost !== "www.careers-page.com") {
    const hostSubdomain = String(finalHost.split(".")[0] || "").trim();
    if (hostSubdomain) slugCandidates.push(hostSubdomain);
  }

  if (fallback.domainSlug) slugCandidates.push(String(fallback.domainSlug));

  let domainSlug = "";
  for (const candidate of slugCandidates) {
    const normalized = String(candidate || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9\-_.]/gi, "");
    if (!normalized || normalized === "job" || normalized === "jobs" || normalized === "www") continue;
    domainSlug = normalized;
    break;
  }

  const protocol = String(finalParsed?.protocol || "https:");
  const hostWithPort = String(finalParsed?.host || fallback.host || "www.careers-page.com");
  const boardUrl =
    finalHost === "www.careers-page.com"
      ? `${protocol}//${hostWithPort}/${domainSlug || String(fallback.domainSlug || "").toLowerCase()}/`
      : finalHost.endsWith(".careers-page.com")
        ? `${protocol}//${hostWithPort}/`
        : String(fallback.boardUrl || "");

  const resolvedSlug = domainSlug || String(fallback.domainSlug || "").toLowerCase();

  return {
    ...fallback,
    host: finalHost || String(fallback.host || "").toLowerCase(),
    domainSlug: resolvedSlug,
    domainSlugLower: resolvedSlug,
    publicBaseUrl: publicBaseUrl || "https://www.careers-page.com",
    boardUrl: boardUrl || String(fallback.boardUrl || ""),
    careersUrl: boardUrl || String(fallback.careersUrl || ""),
    jobsApiUrl: resolvedSlug
      ? `${publicBaseUrl || "https://www.careers-page.com"}/api/v1.0/c/${encodeURIComponent(resolvedSlug)}/jobs/`
      : String(fallback.jobsApiUrl || "")
  };
}

function buildManatalJobPostingUrl(config, item) {
  const posting = item && typeof item === "object" ? item : {};

  for (const key of ["url", "job_url", "apply_url", "public_url"]) {
    const raw = String(posting?.[key] || "").trim();
    if (!raw) continue;
    try {
      return new URL(raw, `${String(config?.boardUrl || config?.baseOrigin || "").replace(/\/+$/, "")}/`).toString();
    } catch {
      continue;
    }
  }

  const hash = String(posting?.hash || "").trim();
  const domainSlug = String(config?.domainSlug || "").trim();
  const publicBaseUrl = String(config?.publicBaseUrl || "https://www.careers-page.com").replace(/\/+$/, "");
  if (hash && domainSlug) {
    return `${publicBaseUrl}/${domainSlug}/job/${encodeURIComponent(hash)}`;
  }

  return String(config?.boardUrl || "").trim();
}

function parseManatalPostingsFromApi(companyNameForPostings, config, responseJson) {
  const results = Array.isArray(responseJson?.results) ? responseJson.results : [];
  const postings = [];
  const seenUrls = new Set();

  for (const job of results) {
    const item = job && typeof job === "object" ? job : {};
    const jobUrl = buildManatalJobPostingUrl(config, item);
    if (!jobUrl || seenUrls.has(jobUrl)) continue;

    const locationDisplay = cleanManatalText(item?.location_display || "");
    const locationParts = [
      cleanManatalText(item?.city || ""),
      cleanManatalText(item?.state || ""),
      cleanManatalText(item?.country || "")
    ].filter(Boolean);
    const location = locationDisplay || locationParts.join(", ");

    let postingDate = null;
    for (const dateField of [
      "last_published_at",
      "published_at",
      "posting_date",
      "posted_date",
      "updated_at",
      "created_at"
    ]) {
      const candidate = cleanManatalText(item?.[dateField] || "");
      if (!candidate) continue;
      postingDate = candidate;
      break;
    }

    postings.push({
      company_name: companyNameForPostings,
      position_name: cleanManatalText(item?.position_name || item?.title || "") || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location: location || null,
      department: cleanManatalText(item?.organization_name || "") || null
    });
    seenUrls.add(jobUrl);
  }

  return postings;
}

function parseManatalPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();

  const cardPattern = /<article[^>]*class=['"][^'"]*\bjob-card\b[^'"]*['"][^>]*>([\s\S]*?)<\/article>/gi;
  let cardMatch = cardPattern.exec(source);
  while (cardMatch) {
    const cardHtml = String(cardMatch[1] || "");
    const href = String(
      cardHtml.match(/<a[^>]*class=['"][^'"]*\bjob-title-link\b[^'"]*['"][^>]*href=['"]([^'"]+)['"]/i)?.[1] || ""
    ).trim();
    const title = cleanManatalText(
      cardHtml.match(/<h[1-6][^>]*class=['"][^'"]*\bjob-title\b[^'"]*['"][^>]*>([\s\S]*?)<\/h[1-6]>/i)?.[1] || ""
    );
    const looksLikeTemplateHref =
      /^getJobUrl\s*\(/i.test(href) ||
      href.includes("[[") ||
      href.includes("]]") ||
      href.includes("{{") ||
      href.includes("}}");
    const looksLikeTemplateTitle = title.includes("[[") || title.includes("]]");
    if (!href || !title || looksLikeTemplateHref || looksLikeTemplateTitle) {
      cardMatch = cardPattern.exec(source);
      continue;
    }

    let jobUrl = "";
    try {
      jobUrl = new URL(href, `${String(config?.boardUrl || config?.baseOrigin || "").replace(/\/+$/, "")}/`).toString();
    } catch {
      cardMatch = cardPattern.exec(source);
      continue;
    }
    if (!jobUrl || seenUrls.has(jobUrl)) {
      cardMatch = cardPattern.exec(source);
      continue;
    }

    const location = cleanManatalText(cardHtml.match(/<li[^>]*>[\s\S]*?<span>\s*([\s\S]*?)\s*<\/span>\s*<\/li>/i)?.[1] || "");
    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: null,
      location: location || null,
      department: null
    });
    seenUrls.add(jobUrl);
    cardMatch = cardPattern.exec(source);
  }

  if (postings.length > 0) return postings;

  const oldItemPattern = /<li[^>]*class=['"][^'"]*\bmedia\b[^'"]*['"][^>]*>([\s\S]*?)<\/li>/gi;
  let oldItemMatch = oldItemPattern.exec(source);
  while (oldItemMatch) {
    const itemHtml = String(oldItemMatch[1] || "");
    const href = String(itemHtml.match(/<a[^>]*href=['"]([^'"]+)['"][^>]*>/i)?.[1] || "").trim();
    const title = cleanManatalText(
      itemHtml.match(/<h[1-6][^>]*class=['"][^'"]*\bjob-position-break\b[^'"]*['"][^>]*>([\s\S]*?)<\/h[1-6]>/i)?.[1] || ""
    );
    const looksLikeTemplateHref =
      /^getJobUrl\s*\(/i.test(href) ||
      href.includes("[[") ||
      href.includes("]]") ||
      href.includes("{{") ||
      href.includes("}}");
    const looksLikeTemplateTitle = title.includes("[[") || title.includes("]]");
    if (!href || !title || looksLikeTemplateHref || looksLikeTemplateTitle) {
      oldItemMatch = oldItemPattern.exec(source);
      continue;
    }

    let jobUrl = "";
    try {
      jobUrl = new URL(href, `${String(config?.boardUrl || config?.baseOrigin || "").replace(/\/+$/, "")}/`).toString();
    } catch {
      oldItemMatch = oldItemPattern.exec(source);
      continue;
    }
    if (!jobUrl || seenUrls.has(jobUrl)) {
      oldItemMatch = oldItemPattern.exec(source);
      continue;
    }

    const location = cleanManatalText(itemHtml.match(/fa-map-marker-alt[^<]*<\/i>\s*([\s\S]*?)<\/span>/i)?.[1] || "");
    const department = cleanManatalText(itemHtml.match(/fa-building[^<]*<\/i>\s*([\s\S]*?)<\/span>/i)?.[1] || "");

    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: null,
      location: location || null,
      department: department || null
    });
    seenUrls.add(jobUrl);
    oldItemMatch = oldItemPattern.exec(source);
  }

  return postings;
}

function parseCareerpuckPostingsFromApi(companyNameForPostings, responseJson) {
  const jobs = Array.isArray(responseJson?.jobs) ? responseJson.jobs : [];
  const postings = [];
  const seenUrls = new Set();

  for (const job of jobs) {
    const status = String(job?.status || "").trim().toLowerCase();
    if (status && status !== "public") continue;

    const publicUrl = String(job?.publicUrl || "").trim();
    const applyUrl = String(job?.applyUrl || "").trim();
    const jobUrl = publicUrl || applyUrl;
    if (!jobUrl || seenUrls.has(jobUrl)) continue;

    const title = String(job?.title || "").trim() || "Untitled Position";
    const location = String(job?.location || "").trim() || null;
    const postingDate = String(job?.postedAt || "").trim() || null;
    const departmentNames = Array.isArray(job?.departments)
      ? job.departments
          .map((item) => String(item?.name || "").trim())
          .filter(Boolean)
      : [];

    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location,
      department: departmentNames.length > 0 ? departmentNames.join(" / ") : null
    });
    seenUrls.add(jobUrl);
  }

  return postings;
}

function parseFountainPostingsFromApi(companyNameForPostings, config, responseJson) {
  const openings = Array.isArray(responseJson?.openings) ? responseJson.openings : [];
  const postings = [];
  const seenUrls = new Set();

  for (const opening of openings) {
    const item = opening && typeof opening === "object" ? opening : {};
    const toParam = String(item?.to_param || "").trim();
    const itemUrl = toParam ? `${config.boardUrl}/${toParam}` : config.boardUrl;
    if (!itemUrl || seenUrls.has(itemUrl)) continue;

    postings.push({
      company_name: companyNameForPostings,
      position_name: String(item?.title || "").trim() || "Untitled Position",
      job_posting_url: itemUrl,
      posting_date:
        String(item?.posted_at || item?.created_at || item?.updated_at || item?.published_at || "").trim() || null,
      location:
        String(item?.location_name || item?.location_address || "").trim() || null,
      employment_type: String(item?.job_type || "").trim() || null
    });
    seenUrls.add(itemUrl);
  }

  return postings;
}

function parseBambooHrPostingsFromApi(companyNameForPostings, config, responseJson) {
  const result = Array.isArray(responseJson?.result) ? responseJson.result : [];
  const postings = [];
  const seenUrls = new Set();

  for (const row of result) {
    const item = row && typeof row === "object" ? row : {};
    const postingId = String(item?.id || "").trim();
    const itemUrlRaw = String(item?.url || item?.jobUrl || item?.applyUrl || "").trim();
    const jobUrl = itemUrlRaw
      ? new URL(itemUrlRaw, `${config.baseOrigin || config.boardUrl || ""}/`).toString()
      : postingId
        ? `${config.boardUrl}/${encodeURIComponent(postingId)}`
        : config.boardUrl;
    if (!jobUrl || seenUrls.has(jobUrl)) continue;

    const locationObject = item?.location && typeof item.location === "object" ? item.location : {};
    const atsLocationObject = item?.atsLocation && typeof item.atsLocation === "object" ? item.atsLocation : {};
    const city = String(locationObject?.city || atsLocationObject?.city || "").trim();
    const state = String(locationObject?.state || atsLocationObject?.state || atsLocationObject?.province || "").trim();
    const country = String(atsLocationObject?.country || "").trim();
    const cityState = [city, state].filter(Boolean).join(", ");
    const location = cityState || [city, country].filter(Boolean).join(", ") || (item?.isRemote ? "Remote" : null);

    const postingDate =
      String(item?.postingDate || item?.postedDate || item?.publishDate || item?.createdDate || item?.updatedDate || "").trim() ||
      null;

    postings.push({
      company_name: companyNameForPostings,
      position_name: String(item?.jobOpeningName || item?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location,
      department: String(item?.departmentLabel || item?.department || "").trim() || null,
      employment_type: String(item?.employmentStatusLabel || item?.employmentStatus || "").trim() || null
    });
    seenUrls.add(jobUrl);
  }

  return postings;
}

function parseTalexioPostingsFromApi(companyNameForPostings, config, responseJson) {
  const vacancies = Array.isArray(responseJson?.vacancies) ? responseJson.vacancies : [];
  const postings = [];
  const seenUrls = new Set();

  for (const vacancy of vacancies) {
    const item = vacancy && typeof vacancy === "object" ? vacancy : {};
    const vacancyId = String(item?.id || "").trim();
    const itemUrlRaw = String(item?.url || item?.jobUrl || item?.vacancyUrl || item?.applyUrl || "").trim();
    const itemUrl = itemUrlRaw
      ? new URL(itemUrlRaw, `${config.baseOrigin || config.jobsUrl || ""}/`).toString()
      : vacancyId
        ? `${config.jobsUrl}?vacancyId=${encodeURIComponent(vacancyId)}`
        : "";
    if (!itemUrl || seenUrls.has(itemUrl)) continue;

    const workLocation = String(item?.workLocation || "").trim();
    const country = String(item?.country || "").trim();
    const location = [workLocation, country].filter(Boolean).join(", ");
    const postingDate = String(item?.publishDate || "").trim() || null;

    postings.push({
      company_name: companyNameForPostings,
      position_name: String(item?.title || "").trim() || "Untitled Position",
      job_posting_url: itemUrl,
      posting_date: postingDate,
      location: location || null,
      reference: String(item?.reference || "").trim() || null,
      department: String(item?.department || "").trim() || null,
      employment_type: String(item?.jobType || "").trim() || null
    });
    seenUrls.add(itemUrl);
  }

  return postings;
}

function cleanSapHrCloudText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function firstSapHrCloudTextValue(value) {
  if (Array.isArray(value)) {
    for (const entry of value) {
      const cleaned = cleanSapHrCloudText(entry);
      if (cleaned) return cleaned;
    }
    return "";
  }
  return cleanSapHrCloudText(value);
}

function buildSapHrCloudJobUrl(config, item = {}, locale = "en_US") {
  const id = cleanSapHrCloudText(item?.id || "");
  if (!id) return "";

  const slugSourceRaw =
    cleanSapHrCloudText(item?.unifiedUrlTitle || "") ||
    cleanSapHrCloudText(item?.urlTitle || "") ||
    cleanSapHrCloudText(item?.unifiedStandardTitle || "") ||
    "untitled";
  let slugSource = slugSourceRaw;
  try {
    slugSource = decodeURIComponent(slugSourceRaw);
  } catch {
    slugSource = slugSourceRaw;
  }
  const slug = encodeURIComponent(
    String(slugSource || "")
      .replace(/[\\/]+/g, "-")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "") || "untitled"
  );
  const localeValue = String(locale || config?.localeFromUrl || "en_US").trim() || "en_US";
  return `${config.baseOrigin}/job/${slug}/${encodeURIComponent(id)}-${encodeURIComponent(localeValue)}`;
}

function parseSapHrCloudPostingsFromApi(companyNameForPostings, config, responseJson, locale = "en_US") {
  const jobSearchResult = Array.isArray(responseJson?.jobSearchResult) ? responseJson.jobSearchResult : [];
  const postings = [];
  const seenUrls = new Set();

  for (const rawItem of jobSearchResult) {
    const item =
      rawItem && typeof rawItem === "object"
        ? rawItem.response && typeof rawItem.response === "object"
          ? rawItem.response
          : rawItem
        : {};

    const absoluteUrlRaw = String(item?.jobUrl || item?.url || item?.applyUrl || "").trim();
    const jobUrl = absoluteUrlRaw
      ? new URL(absoluteUrlRaw, `${config.baseOrigin}/`).toString()
      : buildSapHrCloudJobUrl(config, item, locale);
    if (!jobUrl || seenUrls.has(jobUrl)) continue;

    const locationFromCoordinates = Array.isArray(item?.jobLocationShortWithCoordinates)
      ? firstSapHrCloudTextValue(item.jobLocationShortWithCoordinates.map((entry) => entry?.value))
      : "";
    const location =
      firstSapHrCloudTextValue(item?.jobLocationShort) ||
      locationFromCoordinates ||
      firstSapHrCloudTextValue(item?.jobLocationState) ||
      firstSapHrCloudTextValue(item?.jobLocationCountry) ||
      null;
    const department =
      firstSapHrCloudTextValue(item?.filter8) ||
      firstSapHrCloudTextValue(item?.filter2) ||
      firstSapHrCloudTextValue(item?.businessUnit_obj) ||
      null;
    const postingDate =
      cleanSapHrCloudText(
        item?.unifiedStandardStart || item?.postedDate || item?.publishDate || item?.startDate || ""
      ) || null;

    postings.push({
      company_name: companyNameForPostings,
      position_name:
        cleanSapHrCloudText(item?.unifiedStandardTitle || item?.title || item?.urlTitle || "") || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location,
      department
    });
    seenUrls.add(jobUrl);
  }

  return postings;
}

function decodeBase64Utf8(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    return Buffer.from(raw, "base64").toString("utf8");
  } catch {
    return "";
  }
}

function extractGemNumericJobId(rawId) {
  const direct = String(rawId || "").trim();
  if (/^\d+$/.test(direct)) return direct;

  const decoded = decodeBase64Utf8(direct);
  const match = decoded.match(/:(\d{2,})$/);
  return String(match?.[1] || "").trim();
}

function buildGemJobPostingUrl(config, posting) {
  const boardUrl = String(config?.boardUrl || "").replace(/\/+$/, "");
  const item = posting && typeof posting === "object" ? posting : {};
  const numericId = extractGemNumericJobId(item?.id);
  const extId = String(item?.extId || "").trim();
  const fallbackId = String(item?.id || "").trim();
  const identifier = numericId || extId || fallbackId;
  if (!boardUrl || !identifier) return boardUrl || "";
  return `${boardUrl}/${encodeURIComponent(identifier)}`;
}

function extractGemLocationLabel(posting) {
  const item = posting && typeof posting === "object" ? posting : {};
  const locations = Array.isArray(item?.locations) ? item.locations : [];
  const values = [];
  const seen = new Set();

  for (const location of locations) {
    const source = location && typeof location === "object" ? location : {};
    const name = String(source?.name || "").trim();
    const city = String(source?.city || "").trim();
    const country = String(source?.isoCountry || "").trim();
    const label = name || [city, country].filter(Boolean).join(", ");
    const normalized = label.toLowerCase();
    if (!label || seen.has(normalized)) continue;
    seen.add(normalized);
    values.push(label);
  }

  if (values.length > 0) return values.join(" / ");

  const locationType = String(item?.job?.locationType || "").trim().toUpperCase();
  if (locationType.includes("REMOTE")) return "Remote";
  return null;
}

function parseGemPostingsFromBatchResponse(companyNameForPostings, config, responseJson) {
  const payload = Array.isArray(responseJson) ? responseJson : [];
  let jobPostings = [];
  for (const item of payload) {
    const data = item && typeof item === "object" ? item.data : null;
    const external = data && typeof data === "object" ? data.oatsExternalJobPostings : null;
    const postings = external && typeof external === "object" ? external.jobPostings : null;
    if (!Array.isArray(postings)) continue;
    jobPostings = postings;
    break;
  }

  const collected = [];
  const seenUrls = new Set();

  for (const posting of jobPostings) {
    const item = posting && typeof posting === "object" ? posting : {};
    const postingUrl = buildGemJobPostingUrl(config, item);
    if (!postingUrl || seenUrls.has(postingUrl)) continue;

    const department = String(item?.job?.department?.name || "").trim();
    collected.push({
      company_name: companyNameForPostings,
      position_name: String(item?.title || "").trim() || "Untitled Position",
      job_posting_url: postingUrl,
      posting_date: null,
      location: extractGemLocationLabel(item),
      department: department || null
    });
    seenUrls.add(postingUrl);
  }

  return collected;
}

function cleanJobApsText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function parseJobApsPostingsFromHtml(companyNameForPostings, _config, pageHtml, baseUrl) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();
  const ignoredTitles = new Set(["application-on-file", "application on-file", "application on file", "applicant profile"]);

  const rowPattern = /<tr\b[^>]*>([\s\S]*?)<\/tr>/gi;
  const titleLinkPattern =
    /<a[^>]*href=['"]([^'"]+)['"][^>]*class=['"][^'"]*\bJobTitle\b[^'"]*['"][^>]*>([\s\S]*?)<\/a>/i;
  const jobNumPattern =
    /<a[^>]*href=['"]([^'"]+)['"][^>]*class=['"][^'"]*\bJobNum\b[^'"]*['"][^>]*>([\s\S]*?)<\/a>/i;
  const locationPattern = /<td[^>]*class=['"][^'"]*\bLocs\b[^'"]*['"][^>]*>([\s\S]*?)<\/td>/i;
  const departmentPattern = /<td[^>]*class=['"][^'"]*\bDept\b[^'"]*['"][^>]*>([\s\S]*?)<\/td>/i;
  const salaryPattern = /<td[^>]*class=['"][^'"]*\bSalary\b[^'"]*['"][^>]*>([\s\S]*?)<\/td>/i;

  let rowMatch = rowPattern.exec(source);
  while (rowMatch) {
    const rowHtml = String(rowMatch[1] || "");
    const titleMatch = rowHtml.match(titleLinkPattern);
    if (!titleMatch?.[1]) {
      rowMatch = rowPattern.exec(source);
      continue;
    }

    const href = decodeHtmlEntities(String(titleMatch[1] || "").trim());
    const title = cleanJobApsText(titleMatch[2] || "") || "Untitled Position";
    if (!href) {
      rowMatch = rowPattern.exec(source);
      continue;
    }
    if (href.toLowerCase().includes("r1=af")) {
      rowMatch = rowPattern.exec(source);
      continue;
    }
    if (ignoredTitles.has(title.toLowerCase())) {
      rowMatch = rowPattern.exec(source);
      continue;
    }

    const jobNumValue = cleanJobApsText(rowHtml.match(jobNumPattern)?.[2] || "");
    if (!jobNumValue || jobNumValue.toLowerCase() === "update at any time") {
      rowMatch = rowPattern.exec(source);
      continue;
    }

    let absoluteUrl = "";
    try {
      absoluteUrl = new URL(href, String(baseUrl || "")).toString();
    } catch {
      rowMatch = rowPattern.exec(source);
      continue;
    }
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      rowMatch = rowPattern.exec(source);
      continue;
    }

    const location = cleanJobApsText(rowHtml.match(locationPattern)?.[1] || "");
    const department = cleanJobApsText(rowHtml.match(departmentPattern)?.[1] || "");
    const salary = cleanJobApsText(rowHtml.match(salaryPattern)?.[1] || "");

    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: absoluteUrl,
      posting_date: null,
      location: location || null,
      department: department || null,
      salary: salary || null,
      external_id: jobNumValue || null
    });
    seenUrls.add(absoluteUrl);
    rowMatch = rowPattern.exec(source);
  }

  return postings;
}

function extractJoinNextDataJsonFromHtml(pageHtml) {
  const source = String(pageHtml || "");
  const match = source.match(
    /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>\s*(\{[\s\S]*?\})\s*<\/script>/i
  );
  if (!match?.[1]) return {};
  try {
    return JSON.parse(String(match[1] || "").trim());
  } catch {
    return {};
  }
}

function cleanJoinText(value) {
  return decodeHtmlEntities(String(value || ""))
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function buildJoinJobUrl(companySlug, idParam) {
  const slug = cleanJoinText(companySlug);
  const jobIdParam = cleanJoinText(idParam);
  if (!slug || !jobIdParam) return "";
  return `https://join.com/companies/${encodeURIComponent(slug)}/${encodeURIComponent(jobIdParam)}`;
}

function parseJoinPostingsFromNextData(companyNameForPostings, companySlug, nextData) {
  const props = nextData && typeof nextData === "object" ? nextData.props : {};
  const pageProps = props && typeof props === "object" ? props.pageProps : {};
  const initialState = pageProps && typeof pageProps === "object" ? pageProps.initialState : {};
  const jobsState = initialState && typeof initialState === "object" ? initialState.jobs : {};
  const items = Array.isArray(jobsState?.items) ? jobsState.items : [];

  const postings = [];
  const seenUrls = new Set();

  for (const job of items) {
    const item = job && typeof job === "object" ? job : {};
    const idParam = cleanJoinText(item?.idParam || "");
    const postingUrl = buildJoinJobUrl(companySlug, idParam);
    if (!postingUrl || seenUrls.has(postingUrl)) continue;

    const city = item?.city && typeof item.city === "object" ? item.city : {};
    const cityName = cleanJoinText(city?.cityName || "");
    const countryName = cleanJoinText(city?.countryName || "");
    const locationParts = [cityName, countryName].filter(Boolean);
    let location = locationParts.join(", ");

    const workplaceType = cleanJoinText(item?.workplaceType || "");
    const remoteType = cleanJoinText(item?.remoteType || "");
    if (!location && workplaceType.toUpperCase() === "REMOTE") {
      location = "Remote";
    } else if (!location && remoteType) {
      location = remoteType;
    }

    const category = item?.category && typeof item.category === "object" ? item.category : {};
    const employmentType = item?.employmentType && typeof item.employmentType === "object" ? item.employmentType : {};

    postings.push({
      company_name: companyNameForPostings,
      position_name: cleanJoinText(item?.title || "") || "Untitled Position",
      job_posting_url: postingUrl,
      posting_date: cleanJoinText(item?.createdAt || "") || null,
      location: location || null,
      department: cleanJoinText(category?.name || "") || null,
      employment_type: cleanJoinText(employmentType?.name || "") || null
    });
    seenUrls.add(postingUrl);
  }

  return postings;
}

function extractTalentreefAliasData(aliasResponse) {
  if (!Array.isArray(aliasResponse) || aliasResponse.length === 0) return { clientId: "", brand: "" };
  const firstItem = aliasResponse[0];
  if (!firstItem || typeof firstItem !== "object") return { clientId: "", brand: "" };

  let clientId = "";
  const clients = Array.isArray(firstItem?.clients) ? firstItem.clients : [];
  if (clients.length > 0 && clients[0] && typeof clients[0] === "object") {
    clientId = String(clients[0]?.legacyClientId || clients[0]?.clientId || "").trim();
  }
  if (!clientId) {
    clientId = String(firstItem?.clientId || "").trim();
  }

  let brand = "";
  const brands = Array.isArray(firstItem?.brands) ? firstItem.brands : [];
  if (brands.length > 0) {
    const firstBrand = brands[0];
    if (firstBrand && typeof firstBrand === "object") {
      brand = String(firstBrand?.name || firstBrand?.brand || firstBrand?.title || "").trim();
    } else {
      brand = String(firstBrand || "").trim();
    }
  }
  if (!brand) {
    brand = String(firstItem?.brand || "").trim();
  }

  return { clientId, brand };
}

function buildTalentreefSearchPayload(clientId, brand = "", from = 0, size = 100) {
  const filters = [
    {
      terms: {
        "clientId.raw": [String(clientId || "").trim()]
      }
    }
  ];

  const normalizedBrand = String(brand || "").trim();
  if (normalizedBrand) {
    filters.push({
      terms: {
        "brand.raw": [normalizedBrand]
      }
    });
  }

  return {
    from: Number(from || 0),
    size: Number(size || 100),
    query: {
      bool: {
        filter: filters
      }
    },
    sort: [
      {
        jobId: {
          order: "desc"
        }
      }
    ]
  };
}

function parseTalentreefPostingsFromSearchResponse(companyNameForPostings, config, responseJson) {
  const hits = Array.isArray(responseJson?.hits?.hits) ? responseJson.hits.hits : [];
  const postings = [];
  const seenUrls = new Set();

  for (const hit of hits) {
    const source = hit && typeof hit === "object" && hit._source && typeof hit._source === "object" ? hit._source : {};
    const rawUrl = String(source?.url || "").trim();
    let postingUrl = "";
    try {
      postingUrl = rawUrl ? new URL(rawUrl, `${String(config?.baseOrigin || "").replace(/\/+$/, "")}/`).toString() : "";
    } catch {
      postingUrl = "";
    }
    if (!postingUrl || seenUrls.has(postingUrl)) continue;

    const address = source?.address && typeof source.address === "object" ? source.address : {};
    const city = String(address?.city || "").trim();
    const state = String(source?.stateOrProvinceFull || source?.stateOrProvince || "").trim();
    const location = [city, state].filter(Boolean).join(", ");
    const department = String(source?.department?.name || source?.category || "").trim();
    const postingDate = String(source?.createdDate || source?.startDate || source?.updatedDate || "").trim() || null;

    postings.push({
      company_name: companyNameForPostings,
      position_name: String(source?.title || source?.positionType || "").trim() || "Untitled Position",
      job_posting_url: postingUrl,
      posting_date: postingDate,
      location: location || null,
      department: department || null,
      employment_type: String(source?.contractType || "").trim() || null
    });
    seenUrls.add(postingUrl);
  }

  return postings;
}

function extractGetroNextDataJsonFromHtml(pageHtml) {
  const source = String(pageHtml || "");
  const match = source.match(
    /<script[^>]*id=["']__NEXT_DATA__["'][^>]*>\s*(\{[\s\S]*?\})\s*<\/script>/i
  );
  if (!match?.[1]) return {};
  try {
    return JSON.parse(String(match[1] || "").trim());
  } catch {
    return {};
  }
}

function parseGetroPostingsFromHtml(companyNameForPostings, _config, pageHtml) {
  const nextData = extractGetroNextDataJsonFromHtml(pageHtml);
  const pageProps = nextData?.props?.pageProps && typeof nextData.props.pageProps === "object"
    ? nextData.props.pageProps
    : {};
  const initialState = pageProps?.initialState && typeof pageProps.initialState === "object"
    ? pageProps.initialState
    : {};
  const jobsState = initialState?.jobs && typeof initialState.jobs === "object"
    ? initialState.jobs
    : {};
  const foundJobs = Array.isArray(jobsState?.found) ? jobsState.found : [];

  const postings = [];
  const seenUrls = new Set();

  for (const job of foundJobs) {
    const item = job && typeof job === "object" ? job : {};
    const jobUrl = String(item?.url || "").trim();
    if (!jobUrl || seenUrls.has(jobUrl)) continue;

    const searchableLocations = Array.isArray(item?.searchableLocations) ? item.searchableLocations : [];
    const locations = Array.isArray(item?.locations) ? item.locations : [];
    const locationValue = String(searchableLocations[0] || locations[0] || "").trim();

    const createdAtRaw = item?.createdAt;
    let postingDate = null;
    if (Number.isFinite(Number(createdAtRaw)) && Number(createdAtRaw) > 0) {
      postingDate = String(Math.floor(Number(createdAtRaw)));
    } else if (typeof createdAtRaw === "string" && createdAtRaw.trim()) {
      postingDate = createdAtRaw.trim();
    }

    postings.push({
      company_name: companyNameForPostings,
      position_name: String(item?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location: locationValue || null
    });
    seenUrls.add(jobUrl);
  }

  return postings;
}

function cleanHrmDirectText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeHrmDirectHref(value) {
  return decodeHtmlEntities(String(value || ""))
    .replace(/&#job/gi, "")
    .replace(/#job/gi, "")
    .replace(/&{2,}/g, "&")
    .replace(/[&\s]+$/g, "")
    .trim();
}

function extractHrmDirectCellValue(rowHtml, className) {
  const escapedClassName = escapeRegExp(String(className || "").trim());
  if (!escapedClassName) return "";
  const cellRegex = new RegExp(
    `<td[^>]*class=["'][^"']*\\b${escapedClassName}\\b[^"']*["'][^>]*>([\\s\\S]*?)<\\/td>`,
    "i"
  );
  return String(rowHtml.match(cellRegex)?.[1] || "");
}

function parseHrmDirectPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();
  const rowPattern =
    /<tr[^>]*class=["'][^"']*\breqitem1?\b[^"']*["'][^>]*>([\s\S]*?)<\/tr>/gi;

  let rowMatch = rowPattern.exec(source);
  while (rowMatch) {
    const rowHtml = String(rowMatch[1] || "");
    const titleCell = extractHrmDirectCellValue(rowHtml, "posTitle");
    const titleLinkMatch = titleCell.match(/<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)(?:<\/a>|$)/i);
    const href = normalizeHrmDirectHref(titleLinkMatch?.[1] || "");
    if (!href) {
      rowMatch = rowPattern.exec(source);
      continue;
    }

    const absoluteUrl = new URL(href, `${config.baseOrigin}/employment/`).toString();
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      rowMatch = rowPattern.exec(source);
      continue;
    }

    const title = cleanHrmDirectText(titleLinkMatch?.[2] || titleCell || "");
    const city = cleanHrmDirectText(extractHrmDirectCellValue(rowHtml, "cities"));
    const state = cleanHrmDirectText(extractHrmDirectCellValue(rowHtml, "state"));
    const department = cleanHrmDirectText(extractHrmDirectCellValue(rowHtml, "departments"));
    const postingDate =
      cleanHrmDirectText(extractHrmDirectCellValue(rowHtml, "date")) ||
      cleanHrmDirectText(extractHrmDirectCellValue(rowHtml, "dates")) ||
      null;
    const location = [city, state].filter(Boolean).join(", ");

    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: postingDate,
      location: location || null,
      department: department || null
    });
    seenUrls.add(absoluteUrl);
    rowMatch = rowPattern.exec(source);
  }

  return postings;
}

function cleanTalentlyftText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function extractTalentlyftInitialConfig(pageHtml, fallbackUrl) {
  const source = String(pageHtml || "");
  const parsed = parseUrl(fallbackUrl);
  const websiteUrlDefault = parsed ? `${parsed.protocol}//${parsed.host}` : "";
  const subdomainDefault = parsed ? String(parsed.hostname || "").split(".")[0] : "";

  const pickFirst = (patterns) => {
    for (const pattern of patterns) {
      const match = source.match(pattern);
      if (match?.[1]) return String(match[1]).trim();
    }
    return "";
  };

  const layoutId = pickFirst([/layoutId\s*:\s*['"]([^'"]+)['"]/i, /layoutId\s*=\s*['"]([^'"]+)['"]/i]) || "Jobs-1";
  const themeId = pickFirst([/themeId\s*:\s*['"]([^'"]+)['"]/i, /themeId\s*=\s*['"]([^'"]+)['"]/i]) || "2";
  const language = pickFirst([/language\s*:\s*['"]([^'"]+)['"]/i, /language\s*=\s*['"]([^'"]+)['"]/i]) || "en";
  const subdomain =
    pickFirst([/subdomain\s*:\s*['"]([^'"]+)['"]/i, /subdomain\s*=\s*['"]([^'"]+)['"]/i]) || subdomainDefault;
  const websiteUrl =
    pickFirst([/websiteUrl\s*:\s*['"]([^'"]+)['"]/i, /websiteUrl\s*=\s*['"]([^'"]+)['"]/i]) || websiteUrlDefault;

  return {
    layoutId,
    themeId,
    language,
    subdomain,
    websiteUrl,
    apiUrl: websiteUrl ? `${websiteUrl}/JobList/` : ""
  };
}

function extractTalentlyftTotalPages(fragmentHtml) {
  const source = String(fragmentHtml || "");
  const matches = Array.from(source.matchAll(/data-page=['"](\d+)['"]/gi));
  const pages = matches
    .map((match) => Number(match?.[1] || 0))
    .filter((value) => Number.isFinite(value) && value > 0);
  return pages.length > 0 ? Math.max(...pages) : 1;
}

function parseTalentlyftPostingsFromFragment(companyNameForPostings, config, fragmentHtml) {
  const source = String(fragmentHtml || "");
  const postings = [];
  const seenUrls = new Set();
  const itemPattern =
    /<a[^>]*class=['"][^'"]*\bjobs__box\b[^'"]*['"][^>]*>([\s\S]*?)<\/a>/gi;

  let itemMatch = itemPattern.exec(source);
  while (itemMatch) {
    const blockHtml = String(itemMatch[0] || "");
    const bodyHtml = String(itemMatch[1] || "");

    const href = String(blockHtml.match(/\bhref=['"]([^'"]+)['"]/i)?.[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.baseOrigin || ""}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      itemMatch = itemPattern.exec(source);
      continue;
    }

    const id =
      String(blockHtml.match(/\bdata-job-id=['"](\d+)['"]/i)?.[1] || "").trim() ||
      String(blockHtml.match(/\bid=['"](\d+)['"]/i)?.[1] || "").trim() ||
      absoluteUrl;
    const title = cleanTalentlyftText(bodyHtml.match(/<h3[^>]*class=['"][^'"]*\bjobs__box__heading\b[^'"]*['"][^>]*>([\s\S]*?)<\/h3>/i)?.[1] || "");
    const location = cleanTalentlyftText(bodyHtml.match(/<p[^>]*class=['"][^'"]*\bjobs__box__text\b[^'"]*['"][^>]*>([\s\S]*?)<\/p>/i)?.[1] || "");

    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: null,
      location: location || null
    });
    seenUrls.add(absoluteUrl);
    itemMatch = itemPattern.exec(source);
  }

  return postings;
}

function cleanApplyToJobText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function parseApplyToJobPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();

  const listItemPattern =
    /<li[^>]*class=["'][^"']*\blist-group-item\b[^"']*["'][^>]*>([\s\S]*?)<\/li>/gi;
  const listHeadingPattern =
    /<h3[^>]*class=["'][^"']*\blist-group-item-heading\b[^"']*["'][^>]*>[\s\S]*?<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/i;
  const listLocationPattern = /fa-map-marker[^>]*><\/i>\s*([^<]+)/i;

  let listItemMatch = listItemPattern.exec(source);
  while (listItemMatch) {
    const itemHtml = String(listItemMatch[1] || "");
    const headingMatch = itemHtml.match(listHeadingPattern);
    if (!headingMatch?.[1]) {
      listItemMatch = listItemPattern.exec(source);
      continue;
    }

    const href = String(headingMatch[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.baseOrigin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      listItemMatch = listItemPattern.exec(source);
      continue;
    }

    const locationMatch = itemHtml.match(listLocationPattern);
    const location = locationMatch?.[1] ? cleanApplyToJobText(locationMatch[1]) : null;

    postings.push({
      company_name: companyNameForPostings,
      position_name: cleanApplyToJobText(headingMatch[2]) || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: null,
      location
    });
    seenUrls.add(absoluteUrl);

    listItemMatch = listItemPattern.exec(source);
  }

  const legacyLinkPattern =
    /<a(?=[^>]*\bresumator-job-title-link\b)(?=[^>]*href=["']([^"']+)["'])[^>]*>([\s\S]*?)<\/a>/gi;
  const legacyLocationPattern =
    /<span[^>]*class=["'][^"']*\bresumator-job-location\b[^"']*["'][^>]*>\s*Location:\s*<\/span>\s*([^<]*)/i;

  const legacyMatches = Array.from(source.matchAll(legacyLinkPattern));
  for (let index = 0; index < legacyMatches.length; index += 1) {
    const match = legacyMatches[index];
    const href = String(match?.[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.baseOrigin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) continue;

    const nextStart = index + 1 < legacyMatches.length ? Number(legacyMatches[index + 1].index || 0) : source.length;
    const currentEnd = Number(match.index || 0) + String(match[0] || "").length;
    const searchEnd = Math.min(nextStart, currentEnd + 2500);
    const contextHtml = source.slice(currentEnd, searchEnd);
    const locationMatch = contextHtml.match(legacyLocationPattern);
    const location = locationMatch?.[1] ? cleanApplyToJobText(locationMatch[1]) : null;

    postings.push({
      company_name: companyNameForPostings,
      position_name: cleanApplyToJobText(match?.[2]) || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: null,
      location
    });
    seenUrls.add(absoluteUrl);
  }

  return postings;
}

function cleanTheApplicantManagerText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function parseTheApplicantManagerPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();
  let currentDepartment = "";

  const paragraphPattern =
    /<p[^>]*class=["']([^"']*\bpos_title_list\b[^"']*)["'][^>]*>([\s\S]*?)<\/p>/gi;
  const linkPattern =
    /<a[^>]*class=["'][^"']*\bpos_title_list\b[^"']*["'][^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/i;

  let paragraphMatch = paragraphPattern.exec(source);
  while (paragraphMatch) {
    const classNames = String(paragraphMatch[1] || "").toLowerCase();
    const bodyHtml = String(paragraphMatch[2] || "");

    if (classNames.includes("bold_font")) {
      currentDepartment = cleanTheApplicantManagerText(bodyHtml);
      paragraphMatch = paragraphPattern.exec(source);
      continue;
    }

    const linkMatch = bodyHtml.match(linkPattern);
    if (!linkMatch?.[1]) {
      paragraphMatch = paragraphPattern.exec(source);
      continue;
    }

    const href = String(linkMatch[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.baseOrigin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      paragraphMatch = paragraphPattern.exec(source);
      continue;
    }

    const title = cleanTheApplicantManagerText(linkMatch[2] || "");
    if (!title || title.toLowerCase() === "resume") {
      paragraphMatch = paragraphPattern.exec(source);
      continue;
    }

    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: null,
      location: null,
      department: currentDepartment || null
    });
    seenUrls.add(absoluteUrl);
    paragraphMatch = paragraphPattern.exec(source);
  }

  if (postings.length > 0) return postings;

  const fallbackLinkPattern =
    /<a[^>]*class=["'][^"']*\bpos_title_list\b[^"']*["'][^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let fallbackMatch = fallbackLinkPattern.exec(source);
  while (fallbackMatch) {
    const href = String(fallbackMatch[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.baseOrigin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      fallbackMatch = fallbackLinkPattern.exec(source);
      continue;
    }

    const title = cleanTheApplicantManagerText(fallbackMatch[2] || "");
    if (!title || title.toLowerCase() === "resume") {
      fallbackMatch = fallbackLinkPattern.exec(source);
      continue;
    }

    const contextBefore = source.slice(Math.max(0, Number(fallbackMatch.index || 0) - 1200), Number(fallbackMatch.index || 0));
    const departmentMatches = Array.from(
      contextBefore.matchAll(
        /<p[^>]*class=["'][^"']*\bpos_title_list\b[^"']*\bbold_font\b[^"']*["'][^>]*>([\s\S]*?)<\/p>/gi
      )
    );
    const department =
      departmentMatches.length > 0 ? cleanTheApplicantManagerText(departmentMatches[departmentMatches.length - 1][1] || "") : "";

    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: null,
      location: null,
      department: department || null
    });
    seenUrls.add(absoluteUrl);
    fallbackMatch = fallbackLinkPattern.exec(source);
  }

  return postings;
}

function cleanIcimsText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function cleanBreezyText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .replace(/\s*,\s*/g, ", ")
    .trim();
}

function parseBreezyPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();

  const linkPattern =
    /<a[^>]*href=["']((?:https?:\/\/[^"'<>]+)?\/p\/[^"'<>]+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  const titlePattern = /<h2[^>]*>([\s\S]*?)<\/h2>/i;
  const locationPattern =
    /<li[^>]*class=["'][^"']*\blocation\b[^"']*["'][^>]*>[\s\S]*?<span>([\s\S]*?)<\/span>/i;
  const postedPattern =
    /<li[^>]*class=["'][^"']*(?:posted|created|date)[^"']*["'][^>]*>[\s\S]*?<span>([\s\S]*?)<\/span>/i;
  const departmentPattern =
    /<h2[^>]*class=["'][^"']*\bgroup-header\b[^"']*["'][^>]*>[\s\S]*?<span>([\s\S]*?)<\/span>/gi;

  let linkMatch = linkPattern.exec(source);
  while (linkMatch) {
    const href = String(linkMatch[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.origin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      linkMatch = linkPattern.exec(source);
      continue;
    }

    const linkBody = String(linkMatch[2] || "");
    const titleMatch = linkBody.match(titlePattern);
    const title = cleanBreezyText(titleMatch?.[1] || "");
    if (!title) {
      linkMatch = linkPattern.exec(source);
      continue;
    }

    const locationMatch = linkBody.match(locationPattern);
    const postedMatch = linkBody.match(postedPattern);
    const contextBefore = source.slice(Math.max(0, Number(linkMatch.index || 0) - 3000), Number(linkMatch.index || 0));
    const departmentMatches = Array.from(contextBefore.matchAll(departmentPattern));
    const department =
      departmentMatches.length > 0
        ? cleanBreezyText(departmentMatches[departmentMatches.length - 1][1] || "")
        : "";

    postings.push({
      company_name: companyNameForPostings,
      position_name: title || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: cleanBreezyText(postedMatch?.[1] || "") || null,
      location: cleanBreezyText(locationMatch?.[1] || "") || null,
      department: department || null
    });
    seenUrls.add(absoluteUrl);
    linkMatch = linkPattern.exec(source);
  }

  return postings;
}

function cleanApplicantAiText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function isApplicantAiJobHref(href) {
  const candidate = String(href || "").trim();
  if (!candidate || candidate.startsWith("#") || candidate.toLowerCase().startsWith("mailto:")) {
    return false;
  }

  const parsed = parseUrl(candidate);
  if (parsed?.host) {
    const host = String(parsed.host || "").split(":")[0].toLowerCase();
    if (host !== "applicantai.com" && host !== "www.applicantai.com") {
      return false;
    }
  }

  const path = parsed ? String(parsed.pathname || "") : candidate;
  const pathParts = path
    .split("/")
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  if (pathParts.length < 3) return false;

  return /^\d+$/.test(String(pathParts[pathParts.length - 1] || ""));
}

function parseApplicantAiPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();

  const blockPattern = /<div[^>]*class=["'][^"']*\bmy-4\b[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi;
  const headingLinkPattern = /<h4[^>]*>\s*<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>\s*<\/h4>/i;
  const locationPattern = /<small[^>]*class=["'][^"']*\btext-muted\b[^"']*["'][^>]*>([\s\S]*?)<\/small>/i;

  let blockMatch = blockPattern.exec(source);
  while (blockMatch) {
    const blockHtml = String(blockMatch[1] || "");
    const headingMatch = blockHtml.match(headingLinkPattern);
    if (!headingMatch?.[1]) {
      blockMatch = blockPattern.exec(source);
      continue;
    }

    const href = String(headingMatch[1] || "").trim();
    if (!isApplicantAiJobHref(href)) {
      blockMatch = blockPattern.exec(source);
      continue;
    }

    const absoluteUrl = new URL(href, `${config.baseOrigin}/`).toString();
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      blockMatch = blockPattern.exec(source);
      continue;
    }

    const locationMatch = blockHtml.match(locationPattern);
    const title = cleanApplicantAiText(headingMatch[2] || "") || "Untitled Position";

    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: absoluteUrl,
      posting_date: null,
      location: cleanApplicantAiText(locationMatch?.[1] || "") || null
    });
    seenUrls.add(absoluteUrl);
    blockMatch = blockPattern.exec(source);
  }

  if (postings.length > 0) return postings;

  const fallbackPattern = /<h4[^>]*>\s*<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>\s*<\/h4>/gi;
  let fallbackMatch = fallbackPattern.exec(source);
  while (fallbackMatch) {
    const href = String(fallbackMatch[1] || "").trim();
    if (!isApplicantAiJobHref(href)) {
      fallbackMatch = fallbackPattern.exec(source);
      continue;
    }

    const absoluteUrl = new URL(href, `${config.baseOrigin}/`).toString();
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) {
      fallbackMatch = fallbackPattern.exec(source);
      continue;
    }

    const contextHtml = source.slice(
      Number(fallbackMatch.index || 0),
      Math.min(source.length, Number(fallbackMatch.index || 0) + 700)
    );
    const locationMatch = contextHtml.match(locationPattern);
    const title = cleanApplicantAiText(fallbackMatch[2] || "") || "Untitled Position";

    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: absoluteUrl,
      posting_date: null,
      location: cleanApplicantAiText(locationMatch?.[1] || "") || null
    });
    seenUrls.add(absoluteUrl);
    fallbackMatch = fallbackPattern.exec(source);
  }

  return postings;
}

function extractZohoHiddenInputValue(pageHtml, inputId) {
  const source = String(pageHtml || "");
  const tagMatch = source.match(
    new RegExp(`<input[^>]*\\bid=["']${String(inputId || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}["'][^>]*>`, "is")
  );
  if (!tagMatch?.[0]) return "";

  const valueMatch = tagMatch[0].match(/\bvalue=["']([\s\S]*?)["']/i);
  return String(valueMatch?.[1] || "").trim();
}

function cleanZohoText(value) {
  return decodeHtmlEntities(String(value || "").replace(/<[^>]+>/g, " "))
    .replace(/\s+/g, " ")
    .trim();
}

function extractZohoListUrl(pageHtml, fallbackUrl) {
  const metaPayload = extractZohoHiddenInputValue(pageHtml, "meta");
  if (metaPayload) {
    try {
      const metaData = JSON.parse(decodeHtmlEntities(metaPayload));
      const listUrl = String(metaData?.list_url || "").trim();
      if (listUrl) return listUrl;
    } catch {
      // Continue to fallback extraction paths.
    }
  }

  const ogMatch = String(pageHtml || "").match(
    /<meta[^>]*property=["']og:url["'][^>]*content=["']([^"']+)["']/i
  );
  const ogUrl = String(ogMatch?.[1] || "").trim();
  if (ogUrl) return decodeHtmlEntities(ogUrl);

  const parsed = parseUrl(fallbackUrl);
  if (parsed?.protocol && parsed?.host) {
    return `${parsed.protocol}//${parsed.host}/jobs/Careers`;
  }
  return String(fallbackUrl || "").trim();
}

function buildZohoJobUrl(listUrl, jobId) {
  const parsed = parseUrl(listUrl);
  if (!parsed?.protocol || !parsed?.host) return String(listUrl || "").trim();

  let normalizedPath = String(parsed.pathname || "").replace(/\/+$/, "");
  if (!normalizedPath) normalizedPath = "/jobs/Careers";
  if (!normalizedPath.toLowerCase().includes("/jobs/careers")) {
    normalizedPath = "/jobs/Careers";
  }

  return `${parsed.protocol}//${parsed.host}${normalizedPath}/${encodeURIComponent(String(jobId || "").trim())}`;
}

function parseZohoPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const rawJobsPayload = extractZohoHiddenInputValue(pageHtml, "jobs");
  if (!rawJobsPayload) return [];

  let jobs = [];
  try {
    const parsed = JSON.parse(decodeHtmlEntities(rawJobsPayload));
    if (Array.isArray(parsed)) {
      jobs = parsed;
    }
  } catch {
    return [];
  }

  const listUrl = extractZohoListUrl(pageHtml, config?.careersUrl || config?.origin || "");
  const postings = [];
  const seenIds = new Set();

  for (const job of jobs) {
    if (!job || typeof job !== "object") continue;
    if (job?.Publish === false) continue;

    const jobId = String(job?.id || "").trim();
    if (!jobId || seenIds.has(jobId)) continue;

    const title = cleanZohoText(job?.Posting_Title) || cleanZohoText(job?.Job_Opening_Name) || "Untitled Position";
    const city = cleanZohoText(job?.City);
    const state = cleanZohoText(job?.State);
    const country = cleanZohoText(job?.Country);
    const location = [city, state, country].filter(Boolean).join(", ") || null;
    const postingDate = cleanZohoText(job?.Date_Opened);

    postings.push({
      company_name: companyNameForPostings,
      position_name: title,
      job_posting_url: buildZohoJobUrl(listUrl, jobId),
      posting_date: postingDate || null,
      location,
      department: cleanZohoText(job?.Industry) || null
    });
    seenIds.add(jobId);
  }

  return postings;
}

function ensureIcimsIframeUrl(urlString) {
  const parsed = parseUrl(urlString);
  if (!parsed) return String(urlString || "").trim();
  parsed.searchParams.set("in_iframe", "1");
  return parsed.toString();
}

function extractIcimsIframeUrlFromHtml(pageHtml, baseUrl) {
  const source = String(pageHtml || "");
  const patterns = [
    /icimsFrame\.src\s*=\s*'([^']+)'/i,
    /icimsFrame\.src\s*=\s*"([^"]+)"/i,
    /<iframe[^>]*id=["']icims_content_iframe["'][^>]*src=["']([^"']+)["']/i
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    const rawValue = String(match?.[1] || "").trim();
    if (!rawValue) continue;

    let candidate = decodeHtmlEntities(rawValue).replace(/\\\//g, "/");
    if (!candidate) continue;

    if (candidate.startsWith("//")) {
      const parsedBase = parseUrl(baseUrl);
      const protocol = String(parsedBase?.protocol || "https:");
      candidate = `${protocol}${candidate}`;
    } else if (!/^https?:\/\//i.test(candidate)) {
      try {
        candidate = new URL(candidate, baseUrl).toString();
      } catch {
        continue;
      }
    }

    return ensureIcimsIframeUrl(candidate);
  }

  return ensureIcimsIframeUrl(baseUrl);
}

function extractIcimsLocationFromHtml(sourceHtml) {
  const source = String(sourceHtml || "");
  const patterns = [
    /field-label">Location\s*<\/span>\s*<\/dt>\s*<dd[^>]*class=["'][^"']*iCIMS_JobHeaderData[^"']*["'][^>]*>\s*<span[^>]*>([\s\S]*?)<\/span>/i,
    /glyphicons-map-marker[^>]*>[\s\S]*?<\/dt>\s*<dd[^>]*class=["'][^"']*iCIMS_JobHeaderData[^"']*["'][^>]*>\s*<span[^>]*>([\s\S]*?)<\/span>/i
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    if (!match?.[1]) continue;
    const location = cleanIcimsText(match[1]);
    if (location) return location;
  }

  return null;
}

function extractIcimsPostingDateFromHtml(sourceHtml) {
  const source = String(sourceHtml || "");
  const match = source.match(
    /field-label">Date Posted\s*<\/span>\s*<span[^>]*?(?:title=["']([^"']+)["'])?[^>]*>\s*([^<]*)/i
  );
  const withTitle = String(match?.[1] || "").trim();
  if (withTitle) return withTitle;
  const fallback = cleanIcimsText(match?.[2] || "");
  return fallback || null;
}

function parseIcimsPostingsFromHtml(companyNameForPostings, config, pageHtml) {
  const source = String(pageHtml || "");
  const postings = [];
  const seenUrls = new Set();
  const cardPattern = /<li[^>]*class=["'][^"']*iCIMS_JobCardItem[^"']*["'][^>]*>([\s\S]*?)<\/li>/gi;

  let cardMatch = cardPattern.exec(source);
  while (cardMatch) {
    const cardHtml = String(cardMatch[1] || "");
    const anchorPattern = /<a[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;

    let linkHref = "";
    let linkBody = "";
    let anchorMatch = anchorPattern.exec(cardHtml);
    while (anchorMatch) {
      const href = String(anchorMatch[1] || "").trim();
      if (/\/jobs\/\d+/i.test(href)) {
        linkHref = href;
        linkBody = String(anchorMatch[2] || "");
        break;
      }
      anchorMatch = anchorPattern.exec(cardHtml);
    }

    if (!linkHref) {
      cardMatch = cardPattern.exec(source);
      continue;
    }

    const absoluteUrl = new URL(linkHref, `${config.origin}/`).toString();
    if (!absoluteUrl || seenUrls.has(absoluteUrl) || absoluteUrl.toLowerCase().includes("/jobs/intro")) {
      cardMatch = cardPattern.exec(source);
      continue;
    }

    const titleMatch = linkBody.match(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/i);
    const positionName = cleanIcimsText(titleMatch?.[1] || linkBody) || "Untitled Position";

    postings.push({
      company_name: companyNameForPostings,
      position_name: positionName,
      job_posting_url: absoluteUrl,
      posting_date: extractIcimsPostingDateFromHtml(cardHtml),
      location: extractIcimsLocationFromHtml(cardHtml)
    });
    seenUrls.add(absoluteUrl);
    cardMatch = cardPattern.exec(source);
  }

  if (postings.length > 0) return postings;

  const fallbackLinkPattern = /<a[^>]*href=["']([^"']*\/jobs\/\d+[^"']*)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let fallbackMatch = fallbackLinkPattern.exec(source);
  while (fallbackMatch) {
    const href = String(fallbackMatch[1] || "").trim();
    const absoluteUrl = href ? new URL(href, `${config.origin}/`).toString() : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl) || absoluteUrl.toLowerCase().includes("/jobs/intro")) {
      fallbackMatch = fallbackLinkPattern.exec(source);
      continue;
    }

    const linkBody = String(fallbackMatch[2] || "");
    const titleMatch = linkBody.match(/<h[1-6][^>]*>([\s\S]*?)<\/h[1-6]>/i);
    const positionName = cleanIcimsText(titleMatch?.[1] || linkBody) || "Untitled Position";

    const contextStart = Math.max(0, Number(fallbackMatch.index || 0) - 800);
    const contextEnd = Math.min(source.length, Number(fallbackMatch.index || 0) + String(fallbackMatch[0] || "").length + 2200);
    const contextHtml = source.slice(contextStart, contextEnd);

    postings.push({
      company_name: companyNameForPostings,
      position_name: positionName,
      job_posting_url: absoluteUrl,
      posting_date: extractIcimsPostingDateFromHtml(contextHtml),
      location: extractIcimsLocationFromHtml(contextHtml)
    });
    seenUrls.add(absoluteUrl);
    fallbackMatch = fallbackLinkPattern.exec(source);
  }

  return postings;
}

function extractIcimsNextPageUrlFromHtml(pageHtml, currentUrl) {
  const source = String(pageHtml || "");
  const patterns = [
    /<link[^>]*rel=["']next["'][^>]*href=["']([^"']+)["']/i,
    /<link[^>]*href=["']([^"']+)["'][^>]*rel=["']next["'][^>]*>/i
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    const rawValue = String(match?.[1] || "").trim();
    if (!rawValue) continue;

    let candidate = decodeHtmlEntities(rawValue).replace(/\\\//g, "/");
    if (!candidate) continue;

    if (candidate.startsWith("//")) {
      const parsedCurrent = parseUrl(currentUrl);
      const protocol = String(parsedCurrent?.protocol || "https:");
      candidate = `${protocol}${candidate}`;
    } else if (!/^https?:\/\//i.test(candidate)) {
      try {
        candidate = new URL(candidate, currentUrl).toString();
      } catch {
        continue;
      }
    }

    const normalizedCandidate = ensureIcimsIframeUrl(candidate);
    if (normalizedCandidate && normalizedCandidate !== String(currentUrl || "").trim()) {
      return normalizedCandidate;
    }
  }

  return null;
}

function extractRecruiteePropsFromHtml(pageHtml) {
  const source = String(pageHtml || "");
  const patterns = [
    /data-component=(?:"|')PublicApp(?:"|')[^>]*data-props=(?:"|')([^"']+)(?:"|')/is,
    /data-props=(?:"|')([^"']+)(?:"|')[^>]*data-component=(?:"|')PublicApp(?:"|')/is
  ];

  for (const pattern of patterns) {
    const match = source.match(pattern);
    const encodedProps = String(match?.[1] || "");
    if (!encodedProps) continue;

    const decodedProps = decodeHtmlEntities(encodedProps);
    try {
      const parsedProps = JSON.parse(decodedProps);
      if (parsedProps && typeof parsedProps === "object") return parsedProps;
    } catch {
      // Continue with the next extraction pattern.
    }
  }

  return null;
}

function pickRecruiteeTranslation(translations, preferredLangCode = "") {
  const byLang = translations && typeof translations === "object" ? translations : {};
  const candidates = [];
  const preferred = String(preferredLangCode || "").trim();

  if (preferred && byLang[preferred] && typeof byLang[preferred] === "object") {
    candidates.push(byLang[preferred]);
  }
  if (byLang.en && typeof byLang.en === "object") {
    candidates.push(byLang.en);
  }
  for (const value of Object.values(byLang)) {
    if (value && typeof value === "object") candidates.push(value);
  }

  for (const candidate of candidates) {
    if (candidate && typeof candidate === "object") return candidate;
  }

  return {};
}

function extractRecruiteeTitle(offer, preferredLangCode = "") {
  const translation = pickRecruiteeTranslation(offer?.translations, offer?.primaryLangCode || preferredLangCode);
  const title = String(translation?.title || translation?.name || offer?.slug || "").trim();
  return title || "Untitled Position";
}

function buildRecruiteeLocationLabel(location, preferredLangCode = "") {
  const translation = pickRecruiteeTranslation(location?.translations, preferredLangCode);
  const name = String(translation?.name || translation?.city || location?.name || "").trim();
  const country = String(translation?.country || "").trim();
  if (name && country) return `${name}, ${country}`;
  return name || country || null;
}

function buildAshbyJobUrl(organizationHostedJobsPageName, jobId) {
  if (!organizationHostedJobsPageName || !jobId) return "";
  return `https://jobs.ashbyhq.com/${organizationHostedJobsPageName}/${jobId}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toAtsRateLimitKey(value) {
  const key = String(value || "").trim().toLowerCase();
  return key || "default";
}

function getAtsRateLimitState(rateLimitKey) {
  const normalizedKey = toAtsRateLimitKey(rateLimitKey);
  let state = atsRateLimitStateByKey.get(normalizedKey);
  if (!state) {
    state = {
      active: 0,
      queue: [],
      blockedUntilEpochMs: 0
    };
    atsRateLimitStateByKey.set(normalizedKey, state);
  }
  return state;
}

function parseRetryAfterMilliseconds(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.max(0, Math.ceil(seconds * 1000));
  }

  const parsedEpochMs = Date.parse(raw);
  if (!Number.isFinite(parsedEpochMs)) return null;
  return Math.max(0, parsedEpochMs - Date.now());
}

function resolveAtsRateLimitWaitMs(res, fallbackWaitMs) {
  const minimumWaitMs = Math.max(0, Number(fallbackWaitMs || 0));
  const retryAfterMs = parseRetryAfterMilliseconds(res?.headers?.get("retry-after"));
  if (!Number.isFinite(retryAfterMs)) return minimumWaitMs;
  return Math.max(minimumWaitMs, retryAfterMs);
}

async function acquireAtsRequestSlot(rateLimitKey) {
  const state = getAtsRateLimitState(rateLimitKey);
  if (state.active < atsRequestQueueConcurrency) {
    state.active += 1;
    return;
  }
  await new Promise((resolve) => {
    state.queue.push(resolve);
  });
}

function releaseAtsRequestSlot(rateLimitKey) {
  const state = getAtsRateLimitState(rateLimitKey);
  const next = state.queue.shift();
  if (typeof next === "function") {
    next();
    return;
  }
  state.active = Math.max(0, state.active - 1);
}

function markAtsRateLimited(rateLimitKey, waitMs) {
  const state = getAtsRateLimitState(rateLimitKey);
  const ms = Math.max(0, Number(waitMs || 0));
  state.blockedUntilEpochMs = Math.max(state.blockedUntilEpochMs, Date.now() + ms);
}

async function waitForAtsCooldown(rateLimitKey) {
  const state = getAtsRateLimitState(rateLimitKey);
  while (true) {
    const waitMs = Number(state.blockedUntilEpochMs || 0) - Date.now();
    if (waitMs <= 0) return;
    await sleep(waitMs);
  }
}

async function fetchWithAtsRateLimit(rateLimitKey, fallbackWaitMs, url, init = {}) {
  while (true) {
    await acquireAtsRequestSlot(rateLimitKey);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
    try {
      await waitForAtsCooldown(rateLimitKey);
      const res = await fetch(url, {
        ...init,
        signal: controller.signal
      });

      if (res.status === 429) {
        markAtsRateLimited(rateLimitKey, resolveAtsRateLimitWaitMs(res, fallbackWaitMs));
        continue;
      }

      return res;
    } finally {
      clearTimeout(timeout);
      releaseAtsRequestSlot(rateLimitKey);
    }
  }
}

async function fetchWorkdayPage(cxsUrl, limit, offset) {
  const res = await fetchWithAtsRateLimit("workday", WORKDAY_RATE_LIMIT_WAIT_MS, cxsUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      appliedFacets: {},
      limit,
      offset,
      searchText: ""
    })
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Workday request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchAshbyJobBoard(organizationHostedJobsPageName) {
  const res = await fetchWithAtsRateLimit("ashby", ASHBY_RATE_LIMIT_WAIT_MS, ASHBY_API_URL, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      operationName: "ApiJobBoardWithTeams",
      variables: {
        organizationHostedJobsPageName
      },
      query: ASHBY_QUERY
    })
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Ashby request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const data = await res.json();
  if (Array.isArray(data?.errors) && data.errors.length > 0) {
    const firstError = String(data.errors[0]?.message || "Unknown Ashby GraphQL error");
    throw new Error(`Ashby GraphQL error: ${firstError}`);
  }

  return data;
}

async function fetchGreenhouseJobBoard(boardToken) {
  const encodedBoardToken = encodeURIComponent(boardToken);
  const res = await fetchWithAtsRateLimit(
    "greenhouse",
    GREENHOUSE_RATE_LIMIT_WAIT_MS,
    `${GREENHOUSE_API_URL_BASE}/${encodedBoardToken}/jobs?content=true`,
    {
      method: "GET",
      headers: {
        Accept: "application/json"
      }
    }
  );

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Greenhouse request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchLeverJobBoard(organization) {
  const encodedOrganization = encodeURIComponent(organization);
  const res = await fetchWithAtsRateLimit(
    "lever",
    LEVER_RATE_LIMIT_WAIT_MS,
    `${LEVER_API_URL_BASE}/${encodedOrganization}?mode=json`,
    {
      method: "GET",
      headers: {
        Accept: "application/json"
      }
    }
  );

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Lever request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchRecruiteePublicApp(baseUrl) {
  const res = await fetchWithAtsRateLimit("recruitee", RECRUITEE_RATE_LIMIT_WAIT_MS, baseUrl, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Recruitee request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const pageHtml = await res.text();
  const props = extractRecruiteePropsFromHtml(pageHtml);
  if (!props) {
    throw new Error("Recruitee payload not found in PublicApp data-props");
  }
  return props;
}

async function fetchJobviteJobsPage(jobsUrl) {
  const res = await fetchWithAtsRateLimit("jobvite", JOBVITE_RATE_LIMIT_WAIT_MS, jobsUrl, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Jobvite request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchApplicantProJobsPage(jobsUrl) {
  const res = await fetchWithAtsRateLimit("applicantpro", APPLICANTPRO_RATE_LIMIT_WAIT_MS, jobsUrl, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`ApplicantPro page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchApplicantProJobsList(config, domainId) {
  const apiUrl = new URL(`${String(config?.origin || "").replace(/\/+$/, "")}/core/jobs/${encodeURIComponent(domainId)}`);
  apiUrl.searchParams.set("getParams", "{}");

  const res = await fetchWithAtsRateLimit("applicantpro", APPLICANTPRO_RATE_LIMIT_WAIT_MS, apiUrl.toString(), {
    method: "GET",
    headers: {
      Accept: "application/json"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`ApplicantPro jobs request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const payload = await res.json();
  if (payload && typeof payload === "object" && payload.success === false) {
    const message = String(payload?.message || "Unknown ApplicantPro API error");
    throw new Error(`ApplicantPro jobs API returned success=false: ${message}`);
  }
  return payload;
}

async function fetchApplyToJobPage(applyUrl) {
  const res = await fetchWithAtsRateLimit("applytojob", APPLYTOJOB_RATE_LIMIT_WAIT_MS, applyUrl, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`ApplyToJob page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchTheApplicantManagerPage(careersUrl) {
  const res = await fetchWithAtsRateLimit(
    "theapplicantmanager",
    THEAPPLICANTMANAGER_RATE_LIMIT_WAIT_MS,
    careersUrl,
    {
      method: "GET",
      headers: {
        Accept: "text/html,application/xhtml+xml"
      }
    }
  );

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`TheApplicantManager page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchBreezyPortalPage(urlString) {
  const res = await fetchWithAtsRateLimit("breezy", BREEZY_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Breezy page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const finalUrl = String(res.url || urlString || "").trim();
  const finalHost = String(parseUrl(finalUrl)?.hostname || "").toLowerCase();
  if (finalHost === "breezy.hr" || finalHost === "www.breezy.hr") {
    throw new Error(`Breezy URL redirected to main page: ${finalUrl}`);
  }

  return { pageHtml: await res.text(), finalUrl };
}

async function fetchIcimsPage(urlString) {
  const res = await fetchWithAtsRateLimit("icims", ICIMS_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`iCIMS page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchZohoCareersPage(urlString) {
  const res = await fetchWithAtsRateLimit("zoho", ZOHO_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Zoho Recruit page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchApplicantAiCareersPage(urlString) {
  const res = await fetchWithAtsRateLimit("applicantai", APPLICANTAI_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`ApplicantAI page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchCareerplugJobsPage(urlString) {
  const res = await fetchWithAtsRateLimit("careerplug", CAREERPLUG_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`CareerPlug page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchGemJobBoard(config) {
  const payload = [
    {
      operationName: "JobBoardTheme",
      variables: {
        boardId: config.boardId
      },
      query:
        "query JobBoardTheme($boardId: String!) { publicBrandingTheme(externalId: $boardId) { id theme __typename } }"
    },
    {
      operationName: "JobBoardList",
      variables: {
        boardId: config.boardId
      },
      query:
        "query JobBoardList($boardId: String!) { oatsExternalJobPostings(boardId: $boardId) { jobPostings { id extId title locations { id name city isoCountry isRemote extId __typename } job { id department { id name extId __typename } locationType employmentType __typename } __typename } __typename } jobBoardExternal(vanityUrlPath: $boardId) { id teamDisplayName descriptionHtml pageTitle __typename } }"
    }
  ];

  const res = await fetchWithAtsRateLimit("gem", GEM_RATE_LIMIT_WAIT_MS, config.apiUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Gem API request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const responseJson = await res.json();
  if (!Array.isArray(responseJson)) {
    throw new Error("Gem API response is not a JSON array");
  }

  return responseJson;
}

async function fetchJobApsCareersPage(urlString) {
  const res = await fetchWithAtsRateLimit("jobaps", JOBAPS_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`JobAps page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const finalUrl = String(res.url || urlString || "").trim();
  const finalHost = String(parseUrl(finalUrl)?.hostname || "").toLowerCase();
  if (!finalHost.endsWith(".jobapscloud.com")) {
    throw new Error(`JobAps URL redirected to unexpected host: ${finalUrl}`);
  }

  return { pageHtml: await res.text(), finalUrl };
}

async function fetchJoinCompanyPage(urlString) {
  const res = await fetchWithAtsRateLimit("join", JOIN_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`JOIN page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const finalUrl = String(res.url || urlString || "").trim();
  const finalHost = String(parseUrl(finalUrl)?.hostname || "").toLowerCase();
  if (finalHost !== "join.com" && finalHost !== "www.join.com") {
    throw new Error(`JOIN URL redirected to unexpected host: ${finalUrl}`);
  }

  return { pageHtml: await res.text(), finalUrl };
}

async function fetchTalentreefAlias(config) {
  const res = await fetchWithAtsRateLimit("talentreef", TALENTREEF_RATE_LIMIT_WAIT_MS, config.aliasApiUrl, {
    method: "GET",
    headers: {
      Accept: "application/json"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`TalentReef alias request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchTalentreefSearchResults(config, clientId, brand, from = 0, size = 100) {
  const payload = buildTalentreefSearchPayload(clientId, brand, from, size);
  const res = await fetchWithAtsRateLimit("talentreef", TALENTREEF_RATE_LIMIT_WAIT_MS, config.searchApiUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`TalentReef search request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchManatalCareersPage(urlString) {
  const res = await fetchWithAtsRateLimit("manatal", MANATAL_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
  });

  const finalUrl = String(res.url || urlString || "").trim();
  const pageHtml = await res.text();
  return {
    status: Number(res.status || 0),
    finalUrl,
    pageHtml
  };
}

async function fetchManatalJobsApiPage(config, page = 1, pageSize = 50) {
  const jobsApiUrl = String(config?.jobsApiUrl || "").trim();
  if (!jobsApiUrl) {
    throw new Error("Manatal API URL is missing");
  }

  const query = new URLSearchParams({
    page: String(page),
    page_size: String(pageSize),
    ordering: "-is_pinned_in_career_page,-last_published_at"
  }).toString();
  const url = `${jobsApiUrl}${jobsApiUrl.includes("?") ? "&" : "?"}${query}`;

  const res = await fetchWithAtsRateLimit("manatal", MANATAL_RATE_LIMIT_WAIT_MS, url, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*",
      Referer: String(config?.boardUrl || ""),
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    const error = new Error(`Manatal API request failed (${res.status}): ${body.slice(0, 180)}`);
    error.status = Number(res.status || 0);
    throw error;
  }

  return res.json();
}

async function fetchTeamtailorJobsPage(config) {
  const res = await fetchWithAtsRateLimit("teamtailor", TEAMTAILOR_RATE_LIMIT_WAIT_MS, config.jobsUrl, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Teamtailor page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const finalUrl = String(res.url || config.jobsUrl || "").trim();
  const finalHost = String(parseUrl(finalUrl)?.hostname || "").toLowerCase();
  if (!finalHost.endsWith(".teamtailor.com")) {
    throw new Error(`Teamtailor URL redirected to unexpected host: ${finalUrl}`);
  }

  return { pageHtml: await res.text(), finalUrl };
}

async function fetchBambooHrJobBoard(config) {
  const res = await fetchWithAtsRateLimit("bamboohr", BAMBOOHR_RATE_LIMIT_WAIT_MS, config.apiUrl, {
    method: "GET",
    headers: {
      Accept: "application/json, text/plain, */*"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`BambooHR API request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const finalUrl = String(res.url || config.apiUrl || "").trim();
  const finalHost = String(parseUrl(finalUrl)?.hostname || "").toLowerCase();
  if (!finalHost.endsWith(".bamboohr.com") || finalHost === "bamboohr.com" || finalHost === "www.bamboohr.com") {
    throw new Error(`BambooHR URL redirected to unexpected host: ${finalUrl}`);
  }

  return res.json();
}

async function fetchCareerpuckJobBoard(config) {
  const res = await fetchWithAtsRateLimit("careerpuck", CAREERPUCK_RATE_LIMIT_WAIT_MS, config.apiUrl, {
    method: "GET",
    headers: {
      Accept: "application/json"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`CareerPuck API request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchFountainJobBoard(config) {
  const res = await fetchWithAtsRateLimit("fountain", FOUNTAIN_RATE_LIMIT_WAIT_MS, config.apiUrl, {
    method: "GET",
    headers: {
      Accept: "application/json"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Fountain API request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchGetroJobsPage(urlString) {
  const res = await fetchWithAtsRateLimit("getro", GETRO_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Getro page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchHrmDirectJobsPage(urlString) {
  const res = await fetchWithAtsRateLimit("hrmdirect", HRMDIRECT_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`HRMDirect page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const finalUrl = String(res.url || urlString || "").trim();
  const finalHost = String(parseUrl(finalUrl)?.hostname || "").toLowerCase();
  if (!finalHost.endsWith(".hrmdirect.com")) {
    throw new Error(`HRMDirect URL redirected to unexpected host: ${finalUrl}`);
  }

  return { pageHtml: await res.text(), finalUrl };
}

async function fetchTalentlyftLandingPage(urlString) {
  const res = await fetchWithAtsRateLimit("talentlyft", TALENTLYFT_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Talentlyft landing page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  const finalUrl = String(res.url || urlString || "").trim();
  const finalHost = String(parseUrl(finalUrl)?.hostname || "").toLowerCase();
  if (!finalHost.endsWith(".talentlyft.com")) {
    throw new Error(`Talentlyft URL redirected to unexpected host: ${finalUrl}`);
  }

  return { pageHtml: await res.text(), finalUrl };
}

async function fetchTalentlyftJobListFragment(config, page = 1, pageSize = 20) {
  const apiUrl = String(config?.apiUrl || "").trim();
  if (!apiUrl) {
    throw new Error("Talentlyft API URL is missing");
  }

  const params = new URLSearchParams({
    layoutId: String(config?.layoutId || "Jobs-1"),
    websiteUrl: String(config?.websiteUrl || ""),
    themeId: String(config?.themeId || "2"),
    language: String(config?.language || "en"),
    subdomain: String(config?.subdomain || ""),
    page: String(page),
    pageSize: String(pageSize),
    contains: ""
  }).toString();
  const url = `${apiUrl}${apiUrl.includes("?") ? "&" : "?"}${params}`;

  const res = await fetchWithAtsRateLimit("talentlyft", TALENTLYFT_RATE_LIMIT_WAIT_MS, url, {
    method: "GET",
    headers: {
      Accept: "text/html, */*; q=0.01",
      "x-requested-with": "XMLHttpRequest",
      Referer: `${String(config?.websiteUrl || "").replace(/\/+$/, "")}/`,
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Talentlyft JobList request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchTalexioJobsPage(config, page = 1, limit = 10) {
  const apiUrl = String(config?.apiUrl || "").trim();
  if (!apiUrl) {
    throw new Error("Talexio API URL is missing");
  }

  const url = `${apiUrl}?${new URLSearchParams({
    search: "",
    sortBy: "relevance",
    page: String(page),
    limit: String(limit)
  }).toString()}`;

  const res = await fetchWithAtsRateLimit("talexio", TALEXIO_RATE_LIMIT_WAIT_MS, url, {
    method: "GET",
    headers: {
      Accept: "application/json"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Talexio API request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

function buildSapHrCloudSearchPayload(locale = "en_US", pageNumber = 0) {
  const normalizedPage = Math.max(0, Math.floor(Number(pageNumber || 0)));
  return {
    locale: String(locale || "en_US"),
    pageNumber: normalizedPage,
    sortBy: "",
    keywords: "",
    location: "",
    facetFilters: {},
    brand: "",
    skills: [],
    categoryId: 0,
    alertId: "",
    rcmCandidateId: ""
  };
}

async function fetchSapHrCloudJobsPage(config, locale = "en_US", pageNumber = 0) {
  const payload = buildSapHrCloudSearchPayload(locale, pageNumber);
  const res = await fetchWithAtsRateLimit("saphrcloud", SAPHRCLOUD_RATE_LIMIT_WAIT_MS, config.apiUrl, {
    method: "POST",
    headers: {
      Accept: "application/json, text/plain, */*",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`SAP HR Cloud API request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

function buildUltiProSearchPayload(top, skip) {
  return {
    opportunitySearch: {
      Top: Number(top || ULTIPRO_PAGE_SIZE),
      Skip: Number(skip || 0),
      QueryString: "",
      OrderBy: [
        {
          Value: "postedDateDesc",
          PropertyName: "PostedDate",
          Ascending: false
        }
      ],
      Filters: [
        { t: "TermsSearchFilterDto", fieldName: 4, extra: null, values: [] },
        { t: "TermsSearchFilterDto", fieldName: 5, extra: null, values: [] },
        { t: "TermsSearchFilterDto", fieldName: 6, extra: null, values: [] },
        { t: "TermsSearchFilterDto", fieldName: 37, extra: null, values: [] }
      ]
    },
    matchCriteria: {
      PreferredJobs: [],
      Educations: [],
      LicenseAndCertifications: [],
      Skills: [],
      hasNoLicenses: false,
      SkippedSkills: []
    }
  };
}

async function fetchUltiProSearchResults(config, top, skip) {
  const tenantEncoded = encodeURIComponent(String(config?.tenant || "").trim());
  const boardIdEncoded = encodeURIComponent(String(config?.boardId || "").trim());
  const apiUrl = `https://recruiting.ultipro.com/${tenantEncoded}/JobBoard/${boardIdEncoded}/JobBoardView/LoadSearchResults`;
  const payload = buildUltiProSearchPayload(top, skip);

  const res = await fetchWithAtsRateLimit("ultipro", ULTIPRO_RATE_LIMIT_WAIT_MS, apiUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`UltiPro request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchTaleoJobSearchPage(urlString) {
  const res = await fetchWithAtsRateLimit("taleo", TALEO_RATE_LIMIT_WAIT_MS, urlString, {
    method: "GET",
    headers: {
      Accept: "text/html,application/xhtml+xml"
    }
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Taleo page request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function fetchTaleoRestSearchResults(config, portal, tokenName, tokenValue, pageNo) {
  const apiUrl = `${config.baseOrigin}/careersection/rest/jobboard/searchjobs?lang=${encodeURIComponent(
    config.lang
  )}&portal=${encodeURIComponent(portal)}`;
  const payload = buildTaleoRestPayload(pageNo);

  const headers = {
    Accept: "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json",
    "x-requested-with": "XMLHttpRequest",
    tz: "GMT-07:00",
    tzname: "America/Los_Angeles"
  };
  if (tokenName && tokenValue) {
    headers[tokenName] = tokenValue;
  }

  const res = await fetchWithAtsRateLimit("taleo", TALEO_RATE_LIMIT_WAIT_MS, apiUrl, {
    method: "POST",
    headers,
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Taleo REST request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.json();
}

async function fetchTaleoAjaxSearchResults(config, csrfToken = "") {
  const apiUrl = `${config.baseSectionUrl}/jobsearch.ajax`;
  const payload = new URLSearchParams(buildTaleoAjaxPayload(config.lang, csrfToken)).toString();

  const res = await fetchWithAtsRateLimit("taleo", TALEO_RATE_LIMIT_WAIT_MS, apiUrl, {
    method: "POST",
    headers: {
      Accept: "*/*",
      "Content-Type": "application/x-www-form-urlencoded",
      "x-requested-with": "XMLHttpRequest",
      tz: "GMT-07:00",
      tzname: "America/Los_Angeles"
    },
    body: payload
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Taleo AJAX request failed (${res.status}): ${body.slice(0, 180)}`);
  }

  return res.text();
}

async function collectTodayPostingsForWorkdayCompany(company) {
  const config = parseWorkdayCompany(company.url_string);
  if (!config) return [];

  const collected = [];
  let offset = 0;

  for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
    const response = await fetchWorkdayPage(config.cxsUrl, WORKDAY_PAGE_SIZE, offset);
    const postings = Array.isArray(response?.jobPostings) ? response.jobPostings : [];
    if (postings.length === 0) break;

    let todaysOnPage = 0;
    for (const posting of postings) {
      if (!isPostedToday(posting?.postedOn)) continue;
      todaysOnPage += 1;
      const jobUrl = buildJobUrl(config.companyBaseUrl, posting?.externalPath);
      if (!jobUrl) continue;

      collected.push({
        company_name: company.company_name,
        position_name: String(posting?.title || "").trim() || "Untitled Position",
        job_posting_url: jobUrl,
        posting_date: String(posting?.postedOn || "").trim() || null
      });
    }

    if (todaysOnPage === 0 || postings.length < WORKDAY_PAGE_SIZE) break;
    offset += WORKDAY_PAGE_SIZE;
  }

  return collected;
}

async function collectPostingsForAshbyCompany(company) {
  const config = parseAshbyCompany(company.url_string);
  if (!config) return [];

  const response = await fetchAshbyJobBoard(config.organizationHostedJobsPageName);
  const jobPostings = Array.isArray(response?.data?.jobBoard?.jobPostings)
    ? response.data.jobBoard.jobPostings
    : [];

  const collected = [];
  for (const posting of jobPostings) {
    const jobId = String(posting?.id || "").trim();
    if (!jobId) continue;

    const jobUrl = buildAshbyJobUrl(config.organizationHostedJobsPageName, jobId);
    if (!jobUrl) continue;

    collected.push({
      company_name: company.company_name,
      position_name: String(posting?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: null,
      location: extractAshbyLocationName(posting)
    });
  }

  return collected;
}

function extractGreenhouseLocationName(posting) {
  const nestedLocation = String(posting?.location?.name || "").trim();
  if (nestedLocation) return nestedLocation;

  const flatLocation = String(posting?.location || "").trim();
  return flatLocation || null;
}

async function collectPostingsForGreenhouseCompany(company) {
  const config = parseGreenhouseCompany(company.url_string);
  if (!config) return [];

  const response = await fetchGreenhouseJobBoard(config.boardToken);
  const jobPostings = Array.isArray(response?.jobs) ? response.jobs : [];
  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings =
    normalizedCompanyName && normalizedCompanyName.toLowerCase() !== "job-boards"
      ? normalizedCompanyName
      : config.boardTokenLower;

  const collected = [];
  for (const posting of jobPostings) {
    const jobUrl = String(posting?.absolute_url || "").trim();
    if (!jobUrl) continue;

    collected.push({
      company_name: companyNameForPostings,
      position_name: String(posting?.title || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: String(posting?.updated_at || posting?.first_published || "").trim() || null,
      location: extractGreenhouseLocationName(posting)
    });
  }

  return collected;
}

function extractLeverLocationName(posting) {
  const allLocations = Array.isArray(posting?.categories?.allLocations) ? posting.categories.allLocations : [];
  const normalizedAllLocations = allLocations
    .map((entry) => String(entry || "").trim())
    .filter(Boolean);
  if (normalizedAllLocations.length > 0) {
    return normalizedAllLocations.join(" / ");
  }

  const location = String(posting?.categories?.location || "").trim();
  return location || null;
}

async function collectPostingsForLeverCompany(company) {
  const config = parseLeverCompany(company.url_string);
  if (!config) return [];

  const response = await fetchLeverJobBoard(config.organization);
  const jobPostings = Array.isArray(response) ? response : [];
  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings =
    normalizedCompanyName && normalizedCompanyName.toLowerCase() !== "jobs"
      ? normalizedCompanyName
      : config.organizationLower;

  const collected = [];
  for (const posting of jobPostings) {
    const jobUrl = String(posting?.hostedUrl || "").trim();
    if (!jobUrl) continue;

    const createdAt = Number(posting?.createdAt || 0);
    const postingDate =
      Number.isFinite(createdAt) && createdAt > 0 ? new Date(createdAt).toISOString() : null;

    collected.push({
      company_name: companyNameForPostings,
      position_name: String(posting?.text || "").trim() || "Untitled Position",
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location: extractLeverLocationName(posting)
    });
  }

  return collected;
}

async function collectPostingsForJobviteCompany(company) {
  const config = parseJobviteCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings =
    normalizedCompanyName &&
    normalizedCompanyName.toLowerCase() !== "jobs" &&
    normalizedCompanyName.toLowerCase() !== "careers"
      ? normalizedCompanyName
      : config.companySlugLower;

  const pageHtml = await fetchJobviteJobsPage(config.jobsUrl);
  return parseJobvitePostingsFromHtml(companyNameForPostings, config, pageHtml);
}

function extractApplicantProLocationLabel(job) {
  const location = String(job?.jobLocation || "").trim();
  if (location) return location;

  const city = String(job?.city || "").trim();
  const state = String(job?.abbreviation || job?.stateName || "").trim();
  const country = String(job?.iso3 || "").trim();
  const values = [city, state, country].filter(Boolean);
  return values.length > 0 ? values.join(", ") : null;
}

async function collectPostingsForApplicantProCompany(company) {
  const config = parseApplicantProCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const jobsPageHtml = await fetchApplicantProJobsPage(config.jobsUrl);
  const domainId = extractApplicantProDomainId(jobsPageHtml);
  if (!domainId) {
    throw new Error("ApplicantPro domain_id was not found on the jobs page");
  }

  const response = await fetchApplicantProJobsList(config, domainId);
  const jobs = Array.isArray(response?.data?.jobs) ? response.data.jobs : [];
  const collected = [];
  const seenUrls = new Set();

  for (const job of jobs) {
    const rawJobUrl = String(job?.jobUrl || "").trim();
    const fallbackJobId = String(job?.id ?? "").trim();
    const absoluteUrl = rawJobUrl
      ? new URL(rawJobUrl, `${config.origin}/`).toString()
      : fallbackJobId
        ? `${config.origin}/jobs/${encodeURIComponent(fallbackJobId)}`
        : "";
    if (!absoluteUrl || seenUrls.has(absoluteUrl)) continue;

    collected.push({
      company_name: companyNameForPostings,
      position_name: String(job?.title || "").trim() || "Untitled Position",
      job_posting_url: absoluteUrl,
      posting_date: String(job?.startDateRef || "").trim() || null,
      location: extractApplicantProLocationLabel(job)
    });
    seenUrls.add(absoluteUrl);
  }

  return collected;
}

async function collectPostingsForApplyToJobCompany(company) {
  const config = parseApplyToJobCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const pageHtml = await fetchApplyToJobPage(config.applyUrl);
  return parseApplyToJobPostingsFromHtml(companyNameForPostings, config, pageHtml);
}

async function collectPostingsForTheApplicantManagerCompany(company) {
  const config = parseTheApplicantManagerCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.companyCodeLower;
  const pageHtml = await fetchTheApplicantManagerPage(config.careersUrl);
  return parseTheApplicantManagerPostingsFromHtml(companyNameForPostings, config, pageHtml);
}

async function collectPostingsForBreezyCompany(company) {
  const config = parseBreezyCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const { pageHtml, finalUrl } = await fetchBreezyPortalPage(config.portalUrl);
  const parseConfig = {
    ...config,
    origin: `${parseUrl(finalUrl)?.protocol || "https:"}//${parseUrl(finalUrl)?.host || config.host}`
  };
  return parseBreezyPostingsFromHtml(companyNameForPostings, parseConfig, pageHtml);
}

async function collectPostingsForIcimsCompany(company) {
  const config = parseIcimsCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;

  const wrapperHtml = await fetchIcimsPage(config.searchUrl);
  let pageUrl = extractIcimsIframeUrlFromHtml(wrapperHtml, config.searchUrl);
  const collected = [];
  const seenPostingUrls = new Set();
  const seenPageUrls = new Set();

  for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
    const normalizedPageUrl = ensureIcimsIframeUrl(pageUrl);
    if (!normalizedPageUrl || seenPageUrls.has(normalizedPageUrl)) break;
    seenPageUrls.add(normalizedPageUrl);

    const pageHtml = await fetchIcimsPage(normalizedPageUrl);
    const batch = parseIcimsPostingsFromHtml(companyNameForPostings, config, pageHtml);
    for (const posting of batch) {
      const postingUrl = String(posting?.job_posting_url || "").trim();
      if (!postingUrl || seenPostingUrls.has(postingUrl)) continue;
      seenPostingUrls.add(postingUrl);
      collected.push(posting);
    }

    const nextPageUrl = extractIcimsNextPageUrlFromHtml(pageHtml, normalizedPageUrl);
    if (!nextPageUrl) break;
    pageUrl = nextPageUrl;
  }

  return collected;
}

async function collectPostingsForZohoCompany(company) {
  const config = parseZohoCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const pageHtml = await fetchZohoCareersPage(config.careersUrl);
  return parseZohoPostingsFromHtml(companyNameForPostings, config, pageHtml);
}

async function collectPostingsForApplicantAiCompany(company) {
  const config = parseApplicantAiCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.slugLower;
  const pageHtml = await fetchApplicantAiCareersPage(config.careersUrl);
  return parseApplicantAiPostingsFromHtml(companyNameForPostings, config, pageHtml);
}

async function collectPostingsForGemCompany(company) {
  const config = parseGemCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.boardIdLower;
  const responseJson = await fetchGemJobBoard(config);
  return parseGemPostingsFromBatchResponse(companyNameForPostings, config, responseJson);
}

async function collectPostingsForJobApsCompany(company) {
  const config = parseJobApsCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const hostPrefix = String(config.host || "").split(".")[0];
  const companyNameForPostings = normalizedCompanyName || String(hostPrefix || "").toLowerCase();
  const { pageHtml, finalUrl } = await fetchJobApsCareersPage(config.boardUrl);
  return parseJobApsPostingsFromHtml(companyNameForPostings, config, pageHtml, finalUrl || config.boardUrl);
}

async function collectPostingsForJoinCompany(company) {
  const config = parseJoinCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.companySlugLower;
  const { pageHtml, finalUrl } = await fetchJoinCompanyPage(config.boardUrl);
  const finalConfig = parseJoinCompany(finalUrl || config.boardUrl) || config;
  const nextData = extractJoinNextDataJsonFromHtml(pageHtml);
  return parseJoinPostingsFromNextData(companyNameForPostings, finalConfig.companySlug || config.companySlug, nextData);
}

async function collectPostingsForTalentreefCompany(company) {
  const config = parseTalentreefCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.companyNameLower;
  const aliasResponse = await fetchTalentreefAlias(config);
  const { clientId, brand } = extractTalentreefAliasData(aliasResponse);
  if (!clientId) return [];

  const collected = [];
  const seenUrls = new Set();
  const pageSize = 100;
  let totalHits = null;

  for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
    const from = page * pageSize;
    const responseJson = await fetchTalentreefSearchResults(config, clientId, brand, from, pageSize);
    const batch = parseTalentreefPostingsFromSearchResponse(companyNameForPostings, config, responseJson);
    for (const posting of batch) {
      const postingUrl = String(posting?.job_posting_url || "").trim();
      if (!postingUrl || seenUrls.has(postingUrl)) continue;
      seenUrls.add(postingUrl);
      collected.push(posting);
    }

    const totalRaw = responseJson?.hits?.total;
    const totalValue =
      typeof totalRaw === "number"
        ? totalRaw
        : totalRaw && typeof totalRaw === "object"
          ? Number(totalRaw?.value || 0)
          : 0;
    if (Number.isFinite(totalValue) && totalValue >= 0) {
      totalHits = totalValue;
    }
    if (batch.length < pageSize) break;
    if (Number.isFinite(totalHits) && from + pageSize >= Number(totalHits)) break;
  }

  return collected;
}

async function collectPostingsForCareerplugCompany(company) {
  const config = parseCareerplugCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const pageHtml = await fetchCareerplugJobsPage(config.jobsUrl);
  return parseCareerplugPostingsFromHtml(companyNameForPostings, config, pageHtml);
}

async function collectPostingsForBambooHrCompany(company) {
  const config = parseBambooHrCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.companySubdomainLower;
  const responseJson = await fetchBambooHrJobBoard(config);
  return parseBambooHrPostingsFromApi(companyNameForPostings, config, responseJson);
}

async function collectPostingsForManatalCompany(company) {
  const config = parseManatalCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.domainSlugLower;

  const landing = await fetchManatalCareersPage(config.careersUrl || company.url_string);
  const pageHtml = String(landing?.pageHtml || "");
  const runtimeConfig = extractManatalPageRuntimeConfig(pageHtml, config, landing?.finalUrl || config.careersUrl);

  const collected = [];
  const seenUrls = new Set();

  for (let page = 1; page <= MAX_PAGES_PER_COMPANY; page += 1) {
    let responseJson = {};
    try {
      responseJson = await fetchManatalJobsApiPage(runtimeConfig, page, 50);
    } catch (error) {
      const status = Number(error?.status || 0);
      if (status === 404) {
        break;
      }
      if (page > 1) break;
      throw error;
    }

    const batch = parseManatalPostingsFromApi(companyNameForPostings, runtimeConfig, responseJson);
    for (const posting of batch) {
      const postingUrl = String(posting?.job_posting_url || "").trim();
      if (!postingUrl || seenUrls.has(postingUrl)) continue;
      seenUrls.add(postingUrl);
      collected.push(posting);
    }

    const results = Array.isArray(responseJson?.results) ? responseJson.results : [];
    const totalCount = Number(responseJson?.count);
    const nextUrl = String(responseJson?.next || "").trim();
    if (results.length === 0) break;
    if (!nextUrl) break;
    if (Number.isFinite(totalCount) && totalCount >= 0 && collected.length >= totalCount) break;
  }

  if (collected.length > 0) return collected;

  if (pageHtml) {
    const fallbackPostings = parseManatalPostingsFromHtml(companyNameForPostings, runtimeConfig, pageHtml);
    for (const posting of fallbackPostings) {
      const postingUrl = String(posting?.job_posting_url || "").trim();
      if (!postingUrl || seenUrls.has(postingUrl)) continue;
      seenUrls.add(postingUrl);
      collected.push(posting);
    }
  }

  return collected;
}

async function collectPostingsForTeamtailorCompany(company) {
  const config = parseTeamtailorCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const { pageHtml, finalUrl } = await fetchTeamtailorJobsPage(config);
  const finalParsed = parseUrl(finalUrl);
  const parseConfig = {
    ...config,
    baseOrigin: `${finalParsed?.protocol || "https:"}//${finalParsed?.host || config.host}`,
    jobsUrl: finalUrl || config.jobsUrl
  };
  return parseTeamtailorPostingsFromHtml(companyNameForPostings, parseConfig, pageHtml);
}

async function collectPostingsForCareerpuckCompany(company) {
  const config = parseCareerpuckCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.boardSlugLower;
  const responseJson = await fetchCareerpuckJobBoard(config);
  return parseCareerpuckPostingsFromApi(companyNameForPostings, responseJson);
}

async function collectPostingsForFountainCompany(company) {
  const config = parseFountainCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.companySlugLower;
  const responseJson = await fetchFountainJobBoard(config);
  return parseFountainPostingsFromApi(companyNameForPostings, config, responseJson);
}

async function collectPostingsForGetroCompany(company) {
  const config = parseGetroCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const pageHtml = await fetchGetroJobsPage(config.jobsUrl);
  return parseGetroPostingsFromHtml(companyNameForPostings, config, pageHtml);
}

async function collectPostingsForHrmDirectCompany(company) {
  const config = parseHrmDirectCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const { pageHtml, finalUrl } = await fetchHrmDirectJobsPage(config.jobsUrl);
  const finalParsed = parseUrl(finalUrl);
  const parseConfig = {
    ...config,
    baseOrigin: `${finalParsed?.protocol || "https:"}//${finalParsed?.host || config.host}`,
    jobsUrl: finalUrl || config.jobsUrl
  };
  return parseHrmDirectPostingsFromHtml(companyNameForPostings, parseConfig, pageHtml);
}

async function collectPostingsForTalentlyftCompany(company) {
  const config = parseTalentlyftCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;
  const { pageHtml: landingHtml, finalUrl } = await fetchTalentlyftLandingPage(config.careersUrl);
  const initialConfig = extractTalentlyftInitialConfig(landingHtml, finalUrl || config.careersUrl);

  const finalParsed = parseUrl(finalUrl);
  const baseOrigin = `${finalParsed?.protocol || "https:"}//${finalParsed?.host || config.host}`;
  const runtimeConfig = {
    ...config,
    ...initialConfig,
    baseOrigin,
    websiteUrl: String(initialConfig?.websiteUrl || baseOrigin).replace(/\/+$/, ""),
    apiUrl: String(initialConfig?.apiUrl || `${baseOrigin}/JobList/`).replace(/\/+$/, "") + "/"
  };

  const collected = [];
  const seenUrls = new Set();
  let totalPages = 1;

  for (let page = 1; page <= Math.min(MAX_PAGES_PER_COMPANY, totalPages); page += 1) {
    const fragmentHtml = await fetchTalentlyftJobListFragment(runtimeConfig, page, 20);
    const batch = parseTalentlyftPostingsFromFragment(companyNameForPostings, runtimeConfig, fragmentHtml);

    for (const posting of batch) {
      const postingUrl = String(posting?.job_posting_url || "").trim();
      if (!postingUrl || seenUrls.has(postingUrl)) continue;
      seenUrls.add(postingUrl);
      collected.push(posting);
    }

    totalPages = Math.max(totalPages, extractTalentlyftTotalPages(fragmentHtml));
    if (batch.length === 0 && page >= totalPages) break;
  }

  return collected;
}

async function collectPostingsForTalexioCompany(company) {
  const config = parseTalexioCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.subdomainLower;

  const collected = [];
  const seenUrls = new Set();
  const pageSize = 10;
  let totalVacancies = null;

  for (let page = 1; page <= MAX_PAGES_PER_COMPANY; page += 1) {
    const responseJson = await fetchTalexioJobsPage(config, page, pageSize);
    const batch = parseTalexioPostingsFromApi(companyNameForPostings, config, responseJson);
    for (const posting of batch) {
      const postingUrl = String(posting?.job_posting_url || "").trim();
      if (!postingUrl || seenUrls.has(postingUrl)) continue;
      seenUrls.add(postingUrl);
      collected.push(posting);
    }

    const vacancies = Array.isArray(responseJson?.vacancies) ? responseJson.vacancies : [];
    const totalRaw = Number(responseJson?.totalVacancies);
    if (Number.isFinite(totalRaw) && totalRaw >= 0) {
      totalVacancies = totalRaw;
    }

    if (vacancies.length < pageSize) break;
    if (Number.isFinite(totalVacancies) && collected.length >= Number(totalVacancies)) break;
  }

  return collected;
}

async function collectPostingsForSapHrCloudCompany(company) {
  const config = parseSapHrCloudCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.companyNameLower;
  const localeCandidates = Array.from(
    new Set([String(config.localeFromUrl || "").trim(), ...SAPHRCLOUD_LOCALE_CANDIDATES].filter(Boolean))
  );

  let lastShapeError = "";
  for (const locale of localeCandidates) {
    const collected = [];
    const seenUrls = new Set();
    let sawExpectedPayload = false;
    let totalJobs = null;

    for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
      const responseJson = await fetchSapHrCloudJobsPage(config, locale, page);
      const jobSearchResult = Array.isArray(responseJson?.jobSearchResult) ? responseJson.jobSearchResult : null;
      if (!jobSearchResult) {
        lastShapeError = `SAP HR Cloud response did not contain jobSearchResult list for locale ${locale}`;
        break;
      }

      sawExpectedPayload = true;
      const batch = parseSapHrCloudPostingsFromApi(companyNameForPostings, config, responseJson, locale);
      for (const posting of batch) {
        const postingUrl = String(posting?.job_posting_url || "").trim();
        if (!postingUrl || seenUrls.has(postingUrl)) continue;
        seenUrls.add(postingUrl);
        collected.push(posting);
      }

      const totalRaw = Number(responseJson?.totalJobs);
      if (Number.isFinite(totalRaw) && totalRaw >= 0) {
        totalJobs = totalRaw;
      }

      if (jobSearchResult.length === 0) break;
      if (Number.isFinite(totalJobs) && collected.length >= Number(totalJobs)) break;
    }

    if (sawExpectedPayload) {
      return collected;
    }
  }

  if (lastShapeError) {
    throw new Error(lastShapeError);
  }

  return [];
}

async function collectPostingsForRecruiteeCompany(company) {
  const config = parseRecruiteeCompany(company.url_string);
  if (!config) return [];

  const response = await fetchRecruiteePublicApp(config.baseUrl);
  const appConfig = response?.appConfig && typeof response.appConfig === "object" ? response.appConfig : {};
  const preferredLangCode = String(appConfig?.primaryLangCode || "").trim();
  const offers = Array.isArray(appConfig?.offers) ? appConfig.offers : [];
  const locations = Array.isArray(appConfig?.locations) ? appConfig.locations : [];

  const locationById = new Map();
  for (const location of locations) {
    const id = String(location?.id ?? "").trim();
    if (!id) continue;
    const label = buildRecruiteeLocationLabel(location, preferredLangCode);
    if (label) locationById.set(id, label);
  }

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings =
    normalizedCompanyName && normalizedCompanyName.toLowerCase() !== "recruitee"
      ? normalizedCompanyName
      : config.subdomain;

  const collected = [];
  for (const offer of offers) {
    const slug = String(offer?.slug || "").trim();
    const jobUrl = slug ? `${config.baseUrl}/o/${slug}` : config.baseUrl;
    if (!jobUrl) continue;

    const publishedValue =
      offer?.publishedAt ?? offer?.published_at ?? offer?.createdAt ?? offer?.created_at ?? offer?.updatedAt;
    let postingDate = null;
    if (typeof publishedValue === "string" && publishedValue.trim()) {
      postingDate = publishedValue.trim();
    } else if (typeof publishedValue === "number" && Number.isFinite(publishedValue) && publishedValue > 0) {
      postingDate = new Date(publishedValue).toISOString();
    }

    const locationIds = Array.isArray(offer?.locationIds) ? offer.locationIds : [];
    const locationNames = locationIds
      .map((locationId) => locationById.get(String(locationId ?? "").trim()) || "")
      .filter(Boolean);

    collected.push({
      company_name: companyNameForPostings,
      position_name: extractRecruiteeTitle(offer, preferredLangCode),
      job_posting_url: jobUrl,
      posting_date: postingDate,
      location: locationNames.length > 0 ? locationNames.join(" / ") : null
    });
  }

  return collected;
}

function extractUltiProLocationName(opportunity) {
  const locations = Array.isArray(opportunity?.Locations) ? opportunity.Locations : [];
  const values = [];
  const seen = new Set();

  for (const location of locations) {
    const item = location && typeof location === "object" ? location : {};
    const address = item.Address && typeof item.Address === "object" ? item.Address : {};
    const city = String(address.City || "").trim();
    const state = String(address?.State?.Code || "").trim();
    const country = String(address?.Country?.Name || "").trim();
    const fallback = String(item.LocalizedDescription || item.LocalizedName || "").trim();

    const cityState = [city, state].filter(Boolean).join(", ");
    let label = "";
    if (cityState && country) {
      label = `${cityState}, ${country}`;
    } else if (cityState) {
      label = cityState;
    } else if (fallback) {
      label = fallback;
    } else if (country) {
      label = country;
    }

    const normalized = label.toLowerCase();
    if (!label || seen.has(normalized)) continue;
    seen.add(normalized);
    values.push(label);
  }

  return values.length > 0 ? values.join(" / ") : null;
}

async function collectPostingsForUltiProCompany(company) {
  const config = parseUltiProCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.tenantLower;
  const postings = [];
  const seenIds = new Set();
  let skip = 0;

  for (let page = 0; page < MAX_PAGES_PER_COMPANY; page += 1) {
    const response = await fetchUltiProSearchResults(config, ULTIPRO_PAGE_SIZE, skip);
    const opportunities = Array.isArray(response?.opportunities) ? response.opportunities : [];
    if (opportunities.length === 0) break;

    for (const opportunity of opportunities) {
      const opportunityId = String(opportunity?.Id || "").trim();
      if (!opportunityId || seenIds.has(opportunityId)) continue;

      postings.push({
        company_name: companyNameForPostings,
        position_name: String(opportunity?.Title || "").trim() || "Untitled Position",
        job_posting_url: `${config.baseBoardUrl}/OpportunityDetail?opportunityId=${encodeURIComponent(opportunityId)}`,
        posting_date: String(opportunity?.PostedDate || "").trim() || null,
        location: extractUltiProLocationName(opportunity)
      });
      seenIds.add(opportunityId);
    }

    const totalCount = Number(response?.totalCount);
    if (opportunities.length < ULTIPRO_PAGE_SIZE) break;
    if (Number.isFinite(totalCount) && skip + ULTIPRO_PAGE_SIZE >= totalCount) break;
    skip += ULTIPRO_PAGE_SIZE;
  }

  return postings;
}

async function collectPostingsForTaleoCompany(company) {
  const config = parseTaleoCompany(company.url_string);
  if (!config) return [];

  const normalizedCompanyName = String(company?.company_name || "").trim();
  const companyNameForPostings = normalizedCompanyName || config.careerSectionLower;
  const pageHtml = await fetchTaleoJobSearchPage(company.url_string);
  const { portal, tokenName, tokenValue } = extractTaleoRestConfig(pageHtml);
  const postings = [];
  const seenUrls = new Set();

  if (portal) {
    for (let pageNo = 1; pageNo <= MAX_PAGES_PER_COMPANY; pageNo += 1) {
      const response = await fetchTaleoRestSearchResults(config, portal, tokenName, tokenValue, pageNo);
      const requisitions = Array.isArray(response?.requisitionList) ? response.requisitionList : [];
      if (requisitions.length === 0) break;

      const batch = extractTaleoPostingsFromRest(companyNameForPostings, config, requisitions);
      for (const posting of batch) {
        if (seenUrls.has(posting.job_posting_url)) continue;
        seenUrls.add(posting.job_posting_url);
        postings.push(posting);
      }

      const pagingData = response?.pagingData && typeof response.pagingData === "object" ? response.pagingData : {};
      const totalCount = Number(pagingData?.totalCount);
      const pageSizeRaw = Number(pagingData?.pageSize);
      const pageSize = Number.isFinite(pageSizeRaw) && pageSizeRaw > 0 ? pageSizeRaw : requisitions.length;
      if (requisitions.length < pageSize) break;
      if (Number.isFinite(totalCount) && pageNo * pageSize >= totalCount) break;
    }
  }

  if (postings.length > 0) {
    return postings;
  }

  const ajaxText = await fetchTaleoAjaxSearchResults(config, tokenValue);
  const ajaxPostings = extractTaleoPostingsFromAjax(companyNameForPostings, config, ajaxText);
  for (const posting of ajaxPostings) {
    if (seenUrls.has(posting.job_posting_url)) continue;
    seenUrls.add(posting.job_posting_url);
    postings.push(posting);
  }

  return postings;
}

async function collectPostingsForCompany(company) {
  const atsName = String(company?.ATS_name || "").trim().toLowerCase();
  if (atsName === "workday") {
    return collectTodayPostingsForWorkdayCompany(company);
  }
  if (atsName === "ashbyhq") {
    return collectPostingsForAshbyCompany(company);
  }
  if (atsName === "greenhouseio" || atsName === "greenhouse.io" || atsName === "greenhouse") {
    return collectPostingsForGreenhouseCompany(company);
  }
  if (atsName === "leverco" || atsName === "lever.co" || atsName === "lever") {
    return collectPostingsForLeverCompany(company);
  }
  if (atsName === "jobvite" || atsName === "jobvite.com" || atsName === "jobvitecom") {
    return collectPostingsForJobviteCompany(company);
  }
  if (atsName === "applicantpro" || atsName === "applicantpro.com" || atsName === "applicantprocom") {
    return collectPostingsForApplicantProCompany(company);
  }
  if (atsName === "applytojob" || atsName === "applytojob.com" || atsName === "applytojobcom") {
    return collectPostingsForApplyToJobCompany(company);
  }
  if (
    atsName === "theapplicantmanager" ||
    atsName === "theapplicantmanager.com" ||
    atsName === "theapplicantmanagercom"
  ) {
    return collectPostingsForTheApplicantManagerCompany(company);
  }
  if (atsName === "breezy" || atsName === "breezyhr" || atsName === "breezy.hr" || atsName === "breezyhrcom") {
    return collectPostingsForBreezyCompany(company);
  }
  if (atsName === "icims" || atsName === "icims.com" || atsName === "icimscom") {
    return collectPostingsForIcimsCompany(company);
  }
  if (atsName === "zoho" || atsName === "zohorecruit" || atsName === "zohorecruit.com" || atsName === "zohorecruitcom") {
    return collectPostingsForZohoCompany(company);
  }
  if (atsName === "applicantai" || atsName === "applicantai.com" || atsName === "applicantaicom") {
    return collectPostingsForApplicantAiCompany(company);
  }
  if (atsName === "gem" || atsName === "jobs.gem.com" || atsName === "gem.com" || atsName === "gemcom") {
    return collectPostingsForGemCompany(company);
  }
  if (atsName === "jobaps" || atsName === "jobapscloud.com" || atsName === "jobapscloudcom") {
    return collectPostingsForJobApsCompany(company);
  }
  if (atsName === "join" || atsName === "join.com" || atsName === "joincom") {
    return collectPostingsForJoinCompany(company);
  }
  if (
    atsName === "talentreef" ||
    atsName === "jobappnetwork.com" ||
    atsName === "jobappnetworkcom" ||
    atsName === "apply.jobappnetwork.com" ||
    atsName === "applyjobappnetworkcom"
  ) {
    return collectPostingsForTalentreefCompany(company);
  }
  if (atsName === "careerplug" || atsName === "careerplug.com" || atsName === "careerplugcom") {
    return collectPostingsForCareerplugCompany(company);
  }
  if (atsName === "bamboohr" || atsName === "bamboohr.com" || atsName === "bamboohrcom") {
    return collectPostingsForBambooHrCompany(company);
  }
  if (
    atsName === "manatal" ||
    atsName === "manatal.com" ||
    atsName === "manatalcom" ||
    atsName === "careers-page.com" ||
    atsName === "careerspagecom"
  ) {
    return collectPostingsForManatalCompany(company);
  }
  if (atsName === "teamtailor" || atsName === "teamtailor.com" || atsName === "teamtailorcom") {
    return collectPostingsForTeamtailorCompany(company);
  }
  if (atsName === "careerpuck" || atsName === "careerpuck.com" || atsName === "careerpuckcom") {
    return collectPostingsForCareerpuckCompany(company);
  }
  if (atsName === "fountain" || atsName === "fountain.com" || atsName === "fountaincom") {
    return collectPostingsForFountainCompany(company);
  }
  if (atsName === "getro" || atsName === "getro.com" || atsName === "getrocom") {
    return collectPostingsForGetroCompany(company);
  }
  if (atsName === "hrmdirect" || atsName === "hrmdirect.com" || atsName === "hrmdirectcom") {
    return collectPostingsForHrmDirectCompany(company);
  }
  if (atsName === "talentlyft" || atsName === "talentlyft.com" || atsName === "talentlyftcom") {
    return collectPostingsForTalentlyftCompany(company);
  }
  if (atsName === "talexio" || atsName === "talexio.com" || atsName === "talexiocom") {
    return collectPostingsForTalexioCompany(company);
  }
  if (
    atsName === "saphrcloud" ||
    atsName === "saphrcloud.com" ||
    atsName === "saphrcloudcom" ||
    atsName === "jobs.hr.cloud.sap" ||
    atsName === "jobshrcloudsap"
  ) {
    return collectPostingsForSapHrCloudCompany(company);
  }
  if (atsName === "recruiteecom" || atsName === "recruitee.com" || atsName === "recruitee") {
    return collectPostingsForRecruiteeCompany(company);
  }
  if (atsName === "ultipro" || atsName === "ukg") {
    return collectPostingsForUltiProCompany(company);
  }
  if (atsName === "taleo" || atsName === "taleo.net" || atsName === "taleonet") {
    return collectPostingsForTaleoCompany(company);
  }
  return [];
}

async function ensureCompaniesTableSchema() {
  const tableInfo = await db.all(`PRAGMA table_info('companies');`);
  const columns = new Set(tableInfo.map((column) => String(column?.name || "")));
}

async function initDb() {
  db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database
  });

  await db.exec(`
    PRAGMA journal_mode = WAL;

    CREATE TABLE IF NOT EXISTS companies (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_name TEXT NOT NULL,
      url_string TEXT NOT NULL,
      ATS_name TEXT NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_url_string
      ON companies(url_string);

    CREATE INDEX IF NOT EXISTS idx_companies_company_name
      ON companies(company_name);
  `);

  await ensurePostingsTable();
  await ensurePersonalInformationTable();
  await ensureApplicationsTable();
  await ensureBlockedCompaniesTable();
  await ensureSyncServiceSettingsTable();
  await loadSyncServiceSettingsIntoRuntime();
  await ensureCompaniesTableSchema();
}

async function createCanonicalPostingsTable() {
  await db.exec(`
    CREATE TABLE Postings (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_name TEXT NOT NULL,
      position_name TEXT NOT NULL,
      job_posting_url TEXT NOT NULL UNIQUE,
      posting_date TEXT,
      last_seen_epoch INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_postings_company_name
      ON Postings(company_name);

    CREATE INDEX IF NOT EXISTS idx_postings_position_name
      ON Postings(position_name);

    CREATE INDEX IF NOT EXISTS idx_postings_last_seen_epoch
      ON Postings(last_seen_epoch);
  `);
}

async function ensurePostingsTable() {
  const tableInfo = await db.all(`PRAGMA table_info('Postings');`);

  if (!Array.isArray(tableInfo) || tableInfo.length === 0) {
    await createCanonicalPostingsTable();
    return;
  }

  const requiredColumns = new Set(["id", "company_name", "position_name", "job_posting_url", "posting_date"]);
  const existingColumns = new Set(tableInfo.map((column) => String(column.name)));
  const requiredPresent = Array.from(requiredColumns).every((column) => existingColumns.has(column));

  let incompatibleExtraRequiredColumns = false;
  for (const column of tableInfo) {
    const name = String(column.name);
    if (requiredColumns.has(name)) continue;
    if (Number(column.notnull) === 1 && column.dflt_value === null) {
      incompatibleExtraRequiredColumns = true;
      break;
    }
  }

  if (!requiredPresent || incompatibleExtraRequiredColumns) {
    await db.exec(`DROP TABLE IF EXISTS Postings;`);
    await createCanonicalPostingsTable();
    return;
  }

  if (!existingColumns.has("last_seen_epoch")) {
    await db.exec(`ALTER TABLE Postings ADD COLUMN last_seen_epoch INTEGER;`);
    await db.run(`UPDATE Postings SET last_seen_epoch = ? WHERE last_seen_epoch IS NULL;`, [nowEpochSeconds()]);
  }

  await db.exec(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_postings_job_posting_url
      ON Postings(job_posting_url);

    CREATE INDEX IF NOT EXISTS idx_postings_company_name
      ON Postings(company_name);

    CREATE INDEX IF NOT EXISTS idx_postings_position_name
      ON Postings(position_name);

    CREATE INDEX IF NOT EXISTS idx_postings_last_seen_epoch
      ON Postings(last_seen_epoch);
  `);
}

async function ensurePersonalInformationTable() {
  await db.exec(`
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
      education_level TEXT NOT NULL,
      years_of_experience INTEGER NOT NULL
    );
  `);

  const tableInfo = await db.all(`PRAGMA table_info('PersonalInformation');`);
  const existingColumns = new Set(tableInfo.map((column) => String(column?.name || "")));

  if (!existingColumns.has("years_of_experience")) {
    await db.exec(`
      ALTER TABLE PersonalInformation
      ADD COLUMN years_of_experience INTEGER NOT NULL DEFAULT 0;
    `);
  }
}

async function ensureApplicationsTable() {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS applications (
      id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      company_id INTEGER NOT NULL,
      position_name TEXT NOT NULL,
      application_date INTEGER NOT NULL,
      status TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_applications_company_id
      ON applications(company_id);

    CREATE INDEX IF NOT EXISTS idx_applications_application_date
      ON applications(application_date);

    CREATE INDEX IF NOT EXISTS idx_applications_status
      ON applications(status);

    CREATE TABLE IF NOT EXISTS application_attribution (
      application_id INTEGER NOT NULL PRIMARY KEY,
      applied_by_type TEXT NOT NULL,
      applied_by_label TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS posting_application_state (
      job_posting_url TEXT NOT NULL PRIMARY KEY,
      applied INTEGER NOT NULL DEFAULT 0,
      applied_by_type TEXT NOT NULL,
      applied_by_label TEXT NOT NULL,
      applied_at_epoch INTEGER,
      last_application_id INTEGER,
      ignored INTEGER NOT NULL DEFAULT 0,
      ignored_at_epoch INTEGER,
      ignored_by_label TEXT NOT NULL DEFAULT '',
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_posting_application_state_applied
      ON posting_application_state(applied);

    CREATE INDEX IF NOT EXISTS idx_posting_application_state_ignored
      ON posting_application_state(ignored);

    CREATE TABLE IF NOT EXISTS McpSettings (
      id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
      enabled INTEGER NOT NULL DEFAULT 0,
      preferred_agent_name TEXT NOT NULL DEFAULT 'OpenPostings Agent',
      agent_login_email TEXT NOT NULL DEFAULT '',
      agent_login_password TEXT NOT NULL DEFAULT '',
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
    );
  `);

  await db.run(
    `
      INSERT INTO McpSettings (
        id,
        enabled,
        preferred_agent_name,
        agent_login_email,
        mfa_login_email,
        mfa_login_notes,
        dry_run_only,
        require_final_approval,
        max_applications_per_run,
        preferred_search,
        preferred_remote,
        preferred_industries,
        preferred_states,
        preferred_counties,
        instructions_for_agent
      ) VALUES (1, 0, ?, '', '', '', 1, 1, 10, '', 'all', '[]', '[]', '[]', '')
      ON CONFLICT(id) DO NOTHING;
    `,
    [MCP_SETTINGS_DEFAULTS.preferred_agent_name]
  );

  const postingStateColumns = await db.all(`PRAGMA table_info('posting_application_state');`);
  const postingStateColumnNames = new Set(postingStateColumns.map((column) => String(column?.name || "")));
  const mcpSettingsColumns = await db.all(`PRAGMA table_info('McpSettings');`);
  const mcpSettingsColumnNames = new Set(mcpSettingsColumns.map((column) => String(column?.name || "")));

  if (!postingStateColumnNames.has("ignored")) {
    await db.exec(`
      ALTER TABLE posting_application_state
      ADD COLUMN ignored INTEGER NOT NULL DEFAULT 0;
    `);
  }
  if (!postingStateColumnNames.has("ignored_at_epoch")) {
    await db.exec(`
      ALTER TABLE posting_application_state
      ADD COLUMN ignored_at_epoch INTEGER;
    `);
  }
  if (!postingStateColumnNames.has("ignored_by_label")) {
    await db.exec(`
      ALTER TABLE posting_application_state
      ADD COLUMN ignored_by_label TEXT NOT NULL DEFAULT '';
    `);
  }
  await db.exec(`
    CREATE INDEX IF NOT EXISTS idx_posting_application_state_ignored
      ON posting_application_state(ignored);
  `);

  if (!mcpSettingsColumnNames.has("agent_login_password")) {
    await db.exec(`
      ALTER TABLE McpSettings
      ADD COLUMN agent_login_password TEXT NOT NULL DEFAULT '';
    `);
  }
}

async function ensureSyncServiceSettingsTable() {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS SyncServiceSettings (
      id INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),
      ats_request_queue_concurrency INTEGER NOT NULL DEFAULT 1,
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  await db.run(
    `
      INSERT INTO SyncServiceSettings (
        id,
        ats_request_queue_concurrency,
        updated_at
      ) VALUES (1, ?, datetime('now'))
      ON CONFLICT(id) DO NOTHING;
    `,
    [SYNC_SERVICE_SETTINGS_DEFAULTS.ats_request_queue_concurrency]
  );
}

function normalizeCompanyNameForBlockList(value) {
  return normalizeLikeText(value);
}

async function ensureBlockedCompaniesTable() {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS blocked_companies (
      normalized_company_name TEXT NOT NULL PRIMARY KEY,
      company_name TEXT NOT NULL,
      blocked_at_epoch INTEGER NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_blocked_companies_company_name
      ON blocked_companies(company_name);
  `);
}

async function listBlockedCompanies() {
  const rows = await db.all(`
    SELECT normalized_company_name, company_name, blocked_at_epoch
    FROM blocked_companies
    ORDER BY company_name ASC;
  `);

  return rows.map((row) => ({
    normalized_company_name: String(row?.normalized_company_name || ""),
    company_name: String(row?.company_name || ""),
    blocked_at_epoch: Number(row?.blocked_at_epoch || 0)
  }));
}

async function blockCompanyByName(rawCompanyName) {
  const companyName = String(rawCompanyName || "").trim();
  const normalizedCompanyName = normalizeCompanyNameForBlockList(companyName);
  if (!companyName || !normalizedCompanyName) {
    throw new Error("company_name is required");
  }

  await db.run(
    `
      INSERT INTO blocked_companies (
        normalized_company_name,
        company_name,
        blocked_at_epoch
      ) VALUES (?, ?, ?)
      ON CONFLICT(normalized_company_name) DO UPDATE SET
        company_name = excluded.company_name,
        blocked_at_epoch = excluded.blocked_at_epoch;
    `,
    [normalizedCompanyName, companyName, nowEpochSeconds()]
  );

  return db.get(
    `
      SELECT normalized_company_name, company_name, blocked_at_epoch
      FROM blocked_companies
      WHERE normalized_company_name = ?
      LIMIT 1;
    `,
    [normalizedCompanyName]
  );
}

async function unblockCompanyByName(rawCompanyName) {
  const normalizedCompanyName = normalizeCompanyNameForBlockList(rawCompanyName);
  if (!normalizedCompanyName) {
    throw new Error("company_name is required");
  }

  const result = await db.run(
    `
      DELETE FROM blocked_companies
      WHERE normalized_company_name = ?;
    `,
    [normalizedCompanyName]
  );

  return Number(result?.changes || 0) > 0;
}

async function getStoredSyncServiceSettings() {
  const row = await db.get(
    `
      SELECT
        ats_request_queue_concurrency
      FROM SyncServiceSettings
      WHERE id = 1
      LIMIT 1;
    `
  );

  return normalizeSyncServiceSettingsInput(
    {
      ...SYNC_SERVICE_SETTINGS_DEFAULTS,
      ats_request_queue_concurrency: row?.ats_request_queue_concurrency
    },
    SYNC_SERVICE_SETTINGS_DEFAULTS
  );
}

async function loadSyncServiceSettingsIntoRuntime() {
  const stored = await getStoredSyncServiceSettings();
  atsRequestQueueConcurrency = normalizeAtsRequestQueueConcurrency(stored?.ats_request_queue_concurrency);
  return stored;
}

async function getSyncServiceSettings() {
  const stored = await getStoredSyncServiceSettings();
  return {
    ...stored,
    active_ats_request_queue_concurrency: atsRequestQueueConcurrency,
    min_ats_request_queue_concurrency: MIN_ATS_REQUEST_QUEUE_CONCURRENCY,
    max_ats_request_queue_concurrency: MAX_ATS_REQUEST_QUEUE_CONCURRENCY,
    applies_after_service_restart: true
  };
}

async function upsertSyncServiceSettings(input = {}) {
  const existing = await getStoredSyncServiceSettings();
  const normalized = normalizeSyncServiceSettingsInput(input, existing);

  await db.run(
    `
      INSERT INTO SyncServiceSettings (
        id,
        ats_request_queue_concurrency,
        updated_at
      ) VALUES (1, ?, datetime('now'))
      ON CONFLICT(id) DO UPDATE SET
        ats_request_queue_concurrency = excluded.ats_request_queue_concurrency,
        updated_at = datetime('now');
    `,
    [normalized.ats_request_queue_concurrency]
  );

  return getSyncServiceSettings();
}

async function getMcpSettings() {
  const row = await db.get(
    `
      SELECT
        id,
        enabled,
        preferred_agent_name,
        agent_login_email,
        agent_login_password,
        mfa_login_email,
        mfa_login_notes,
        dry_run_only,
        require_final_approval,
        max_applications_per_run,
        preferred_search,
        preferred_remote,
        preferred_industries,
        preferred_states,
        preferred_counties,
        instructions_for_agent
      FROM McpSettings
      WHERE id = 1
      LIMIT 1;
    `
  );

  const settings = normalizeMcpSettingsInput({
    ...MCP_SETTINGS_DEFAULTS,
    enabled: Boolean(Number(row?.enabled || 0)),
    preferred_agent_name: row?.preferred_agent_name,
    agent_login_email: row?.agent_login_email,
    agent_login_password: row?.agent_login_password,
    mfa_login_email: row?.mfa_login_email,
    mfa_login_notes: row?.mfa_login_notes,
    dry_run_only: Boolean(Number(row?.dry_run_only ?? 1)),
    require_final_approval: Boolean(Number(row?.require_final_approval ?? 1)),
    max_applications_per_run: row?.max_applications_per_run,
    preferred_search: row?.preferred_search,
    preferred_remote: row?.preferred_remote,
    preferred_industries: parseJsonArray(row?.preferred_industries),
    preferred_states: parseJsonArray(row?.preferred_states),
    preferred_counties: parseJsonArray(row?.preferred_counties),
    instructions_for_agent: row?.instructions_for_agent
  });

  return settings;
}

async function upsertMcpSettings(input) {
  const normalized = normalizeMcpSettingsInput(input);
  await db.run(
    `
      INSERT INTO McpSettings (
        id,
        enabled,
        preferred_agent_name,
        agent_login_email,
        agent_login_password,
        mfa_login_email,
        mfa_login_notes,
        dry_run_only,
        require_final_approval,
        max_applications_per_run,
        preferred_search,
        preferred_remote,
        preferred_industries,
        preferred_states,
        preferred_counties,
        instructions_for_agent,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
      ON CONFLICT(id) DO UPDATE SET
        enabled = excluded.enabled,
        preferred_agent_name = excluded.preferred_agent_name,
        agent_login_email = excluded.agent_login_email,
        agent_login_password = excluded.agent_login_password,
        mfa_login_email = excluded.mfa_login_email,
        mfa_login_notes = excluded.mfa_login_notes,
        dry_run_only = excluded.dry_run_only,
        require_final_approval = excluded.require_final_approval,
        max_applications_per_run = excluded.max_applications_per_run,
        preferred_search = excluded.preferred_search,
        preferred_remote = excluded.preferred_remote,
        preferred_industries = excluded.preferred_industries,
        preferred_states = excluded.preferred_states,
        preferred_counties = excluded.preferred_counties,
        instructions_for_agent = excluded.instructions_for_agent,
        updated_at = datetime('now');
    `,
    [
      1,
      normalized.enabled ? 1 : 0,
      normalized.preferred_agent_name,
      normalized.agent_login_email,
      normalized.agent_login_password,
      normalized.mfa_login_email,
      normalized.mfa_login_notes,
      normalized.dry_run_only ? 1 : 0,
      normalized.require_final_approval ? 1 : 0,
      normalized.max_applications_per_run,
      normalized.preferred_search,
      normalized.preferred_remote,
      JSON.stringify(normalized.preferred_industries || []),
      JSON.stringify(normalized.preferred_states || []),
      JSON.stringify(normalized.preferred_counties || []),
      normalized.instructions_for_agent
    ]
  );

  return getMcpSettings();
}

async function markPostingAppliedState(payload) {
  const jobPostingUrl = String(payload?.job_posting_url || "").trim();
  if (!jobPostingUrl) return;

  const applied = normalizeBoolean(payload?.applied, true);
  const appliedByType = normalizeAppliedByType(payload?.applied_by_type);
  const appliedByLabel = normalizeAppliedByLabel(payload?.applied_by_label, appliedByType);
  const appliedAtEpoch = parseNonNegativeInteger(payload?.applied_at_epoch) || nowEpochSeconds();
  const lastApplicationId = parseNonNegativeInteger(payload?.last_application_id) || null;

  await db.run(
    `
      INSERT INTO posting_application_state (
        job_posting_url,
        applied,
        applied_by_type,
        applied_by_label,
        applied_at_epoch,
        last_application_id,
        ignored,
        ignored_at_epoch,
        ignored_by_label,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 0, NULL, '', datetime('now'))
      ON CONFLICT(job_posting_url) DO UPDATE SET
        applied = excluded.applied,
        applied_by_type = excluded.applied_by_type,
        applied_by_label = excluded.applied_by_label,
        applied_at_epoch = excluded.applied_at_epoch,
        last_application_id = excluded.last_application_id,
        ignored = 0,
        ignored_at_epoch = NULL,
        ignored_by_label = '',
        updated_at = datetime('now');
    `,
    [jobPostingUrl, applied ? 1 : 0, appliedByType, appliedByLabel, appliedAtEpoch, lastApplicationId]
  );
}

async function setPostingIgnoredState(payload) {
  const jobPostingUrl = String(payload?.job_posting_url || "").trim();
  if (!jobPostingUrl) {
    throw new Error("job_posting_url is required");
  }

  const ignored = normalizeBoolean(payload?.ignored, true);
  const ignoredAtEpoch = parseNonNegativeInteger(payload?.ignored_at_epoch) || nowEpochSeconds();
  const ignoredByLabel = normalizeIgnoredByLabel(payload?.ignored_by_label);

  await db.run(
    `
      INSERT INTO posting_application_state (
        job_posting_url,
        applied,
        applied_by_type,
        applied_by_label,
        applied_at_epoch,
        last_application_id,
        ignored,
        ignored_at_epoch,
        ignored_by_label,
        updated_at
      ) VALUES (?, 0, 'manual', '', NULL, NULL, ?, ?, ?, datetime('now'))
      ON CONFLICT(job_posting_url) DO UPDATE SET
        ignored = excluded.ignored,
        ignored_at_epoch = CASE
          WHEN excluded.ignored = 1 THEN excluded.ignored_at_epoch
          ELSE NULL
        END,
        ignored_by_label = CASE
          WHEN excluded.ignored = 1 THEN excluded.ignored_by_label
          ELSE ''
        END,
        updated_at = datetime('now');
    `,
    [jobPostingUrl, ignored ? 1 : 0, ignoredAtEpoch, ignoredByLabel]
  );

  const row = await db.get(
    `
      SELECT
        job_posting_url,
        applied,
        ignored,
        ignored_at_epoch,
        ignored_by_label
      FROM posting_application_state
      WHERE job_posting_url = ?
      LIMIT 1;
    `,
    [jobPostingUrl]
  );

  return {
    job_posting_url: jobPostingUrl,
    applied: Boolean(Number(row?.applied || 0)),
    ignored: Boolean(Number(row?.ignored || 0)),
    ignored_at_epoch: Number(row?.ignored_at_epoch || 0),
    ignored_by_label: String(row?.ignored_by_label || "")
  };
}

async function enrichPostingsWithApplicationState(items) {
  const rows = Array.isArray(items) ? items : [];
  const urls = rows
    .map((row) => String(row?.job_posting_url || "").trim())
    .filter(Boolean);
  if (urls.length === 0) return rows;

  const uniqueUrls = Array.from(new Set(urls));
  const placeholders = uniqueUrls.map(() => "?").join(", ");
  const stateRows = await db.all(
    `
      SELECT
        job_posting_url,
        applied,
        applied_by_type,
        applied_by_label,
        applied_at_epoch,
        last_application_id,
        ignored,
        ignored_at_epoch,
        ignored_by_label
      FROM posting_application_state
      WHERE job_posting_url IN (${placeholders});
    `,
    uniqueUrls
  );

  const byUrl = new Map();
  for (const row of stateRows) {
    byUrl.set(String(row?.job_posting_url || "").trim(), row);
  }

  return rows.map((item) => {
    const key = String(item?.job_posting_url || "").trim();
    const state = byUrl.get(key);
    const applied = Boolean(Number(state?.applied || 0));
    const ignored = Boolean(Number(state?.ignored || 0));
    const appliedByType = applied ? normalizeAppliedByType(state?.applied_by_type) : "";
    return {
      ...item,
      applied,
      ignored,
      applied_by_type: appliedByType,
      applied_by_label: applied ? normalizeAppliedByLabel(state?.applied_by_label, appliedByType) : "",
      applied_at_epoch: Number(state?.applied_at_epoch || 0),
      last_application_id: Number(state?.last_application_id || 0),
      ignored_at_epoch: Number(state?.ignored_at_epoch || 0),
      ignored_by_label: ignored ? normalizeIgnoredByLabel(state?.ignored_by_label) : ""
    };
  });
}

async function listPostingsWithFilters(options = {}) {
  await pruneExpiredPostings();
  const search = String(options?.search || "").trim();
  const limit = Math.max(1, Math.min(2000, Number(options?.limit || 500)));
  const offset = Math.max(0, Number(options?.offset || 0));
  const sortBy = normalizePostingSort(options?.sort_by);
  const orderByClause = getPostingsOrderByClause(sortBy);
  const atsFilters = normalizeAtsFilters(options?.ats || []);
  const industryKeys = normalizeStringArray(options?.industries).map((key) => normalizeLikeText(key));
  const stateCodes = normalizeStringArray(options?.states).map((state) => state.toUpperCase());
  const countyFilters = parseCountyFilters(normalizeStringArray(options?.counties));
  const remoteFilter = normalizeRemoteFilter(options?.remote);
  const hideNoDate = normalizeBoolean(options?.hide_no_date, false);
  const includeApplied = normalizeBoolean(options?.include_applied, true);
  const includeIgnored = normalizeBoolean(options?.include_ignored, false);
  const hasStructuredFilters =
    atsFilters.length > 0 ||
    industryKeys.length > 0 ||
    stateCodes.length > 0 ||
    countyFilters.length > 0 ||
    remoteFilter !== "all";

  let rows = [];
  if (!search && !hasStructuredFilters) {
    if (includeApplied && includeIgnored) {
      rows = await db.all(
        `
          SELECT id, company_name, position_name, job_posting_url, posting_date, last_seen_epoch
          FROM Postings
          WHERE (? = 0 OR (posting_date IS NOT NULL AND TRIM(posting_date) <> ''))
            AND NOT EXISTS (
              SELECT 1
              FROM blocked_companies b
              WHERE b.normalized_company_name = LOWER(TRIM(Postings.company_name))
            )
          ORDER BY ${orderByClause}
          LIMIT ? OFFSET ?;
        `,
        [hideNoDate ? 1 : 0, limit, offset]
      );
    } else {
      rows = await db.all(
        `
          SELECT p.id, p.company_name, p.position_name, p.job_posting_url, p.posting_date, p.last_seen_epoch
          FROM Postings p
          LEFT JOIN posting_application_state s
            ON s.job_posting_url = p.job_posting_url
            AND (
              (${includeApplied ? 0 : 1} = 1 AND COALESCE(s.applied, 0) = 1)
              OR
              (${includeIgnored ? 0 : 1} = 1 AND COALESCE(s.ignored, 0) = 1)
            )
          WHERE (? = 0 OR (p.posting_date IS NOT NULL AND TRIM(p.posting_date) <> ''))
            AND NOT EXISTS (
              SELECT 1
              FROM blocked_companies b
              WHERE b.normalized_company_name = LOWER(TRIM(p.company_name))
            )
            AND s.job_posting_url IS NULL
          ORDER BY ${orderByClause}
          LIMIT ? OFFSET ?;
        `,
        [hideNoDate ? 1 : 0, limit, offset]
      );
    }
  } else {
    rows = await db.all(
      `
        SELECT id, company_name, position_name, job_posting_url, posting_date, last_seen_epoch
        FROM Postings
        WHERE NOT EXISTS (
          SELECT 1
          FROM blocked_companies b
          WHERE b.normalized_company_name = LOWER(TRIM(Postings.company_name))
        )
        ORDER BY ${orderByClause};
      `
    );
  }

  const enrichedRows = rows.map((row) => ({
    ...row,
    location: inferPostingLocationFromJobUrl(row?.job_posting_url),
    ats: inferAtsFromJobPostingUrl(row?.job_posting_url)
  }));

  const searchTerms = search.toLowerCase().split(/\s+/).filter(Boolean);
  const industryMatchersByKey = await buildIndustryMatchersByKey(industryKeys);

  let items = enrichedRows;
  if (search || hasStructuredFilters) {
    items = enrichedRows.filter((row) => {
      const companyName = String(row?.company_name || "").toLowerCase();
      const positionName = String(row?.position_name || "").toLowerCase();
      const location = String(row?.location || "").toLowerCase();
      const ats = String(row?.ats || "").toLowerCase();

      const matchesSearch = searchTerms.every(
        (term) => companyName.includes(term) || positionName.includes(term) || location.includes(term)
      );
      if (!matchesSearch) return false;

      if (atsFilters.length > 0 && !atsFilters.includes(ats)) return false;

      const matchesIndustry = rowMatchesIndustryLikeParts(
        row?.position_name,
        industryKeys,
        industryMatchersByKey
      );
      if (!matchesIndustry) return false;

      const matchesLocation = rowMatchesLocationFilters(row?.location, stateCodes, countyFilters);
      if (!matchesLocation) return false;

      const matchesRemote = rowMatchesRemoteFilter(row?.location, remoteFilter);
      if (!matchesRemote) return false;

      if (hideNoDate && !String(row?.posting_date || "").trim()) return false;

      return true;
    });
    items = items.slice(offset, offset + limit);
  }

  items = await enrichPostingsWithApplicationState(items);

  if (!includeApplied) {
    items = items.filter((item) => !item.applied);
  }
  if (!includeIgnored) {
    items = items.filter((item) => !item.ignored);
  }

  return {
    items,
    count: items.length,
    limit,
    offset,
    filters: {
      search,
      ats: atsFilters,
      sort_by: sortBy,
      industries: industryKeys,
      states: stateCodes,
      counties: countyFilters.map((filter) =>
        filter?.stateCode ? `${filter.stateCode}|${filter.countyLikePart}` : filter.countyLikePart
      ),
      remote: remoteFilter,
      hide_no_date: hideNoDate,
      include_ignored: includeIgnored
    }
  };
}

function buildMcpRunbook(settings, personalInformation, candidates) {
  const preferredAgent = String(settings?.preferred_agent_name || "OpenPostings Agent").trim();
  const applicantFullName = [
    String(personalInformation?.first_name || "").trim(),
    String(personalInformation?.middle_name || "").trim(),
    String(personalInformation?.last_name || "").trim()
  ]
    .filter(Boolean)
    .join(" ");

  return {
    preferred_agent_name: preferredAgent,
    summary:
      "Use your existing browser/web automation tools to open each job URL, complete the application form, and submit only when allowed by settings and credentials.",
    steps: [
      "Read applicantee information and MCP settings from this payload.",
      "For each candidate posting, open job_posting_url and validate role relevance before applying.",
      "Fill application fields using applicantee information. Keep applicant email separate from agent login email.",
      "If an account or MFA is required, use agent_login_email + agent_login_password for account creation and sign-in flows.",
      "Use the same agent_login_email for MFA/approval flows when required.",
      "Draft a job-specific cover letter aligned to the posting requirements and applicant background.",
      "If dry_run_only is true, stop before final submit and return a dry-run result.",
      "When application is submitted, call record_application_result with commit=true to write outcomes."
    ],
    guardrails: {
      dry_run_only: Boolean(settings?.dry_run_only),
      require_final_approval: Boolean(settings?.require_final_approval)
    },
    applicant_display_name: applicantFullName || "Applicant",
    applicant_email: String(personalInformation?.email || "").trim(),
    agent_login_email: String(settings?.agent_login_email || "").trim(),
    agent_login_password: String(settings?.agent_login_password || ""),
    mfa_login_email: String(settings?.agent_login_email || "").trim(),
    mfa_login_notes: String(settings?.mfa_login_notes || "").trim(),
    custom_instructions: String(settings?.instructions_for_agent || "").trim(),
    candidate_count: Array.isArray(candidates) ? candidates.length : 0
  };
}

function buildCoverLetterDraft(personalInformation, posting, instructions = "") {
  const firstName = String(personalInformation?.first_name || "").trim() || "Applicant";
  const lastName = String(personalInformation?.last_name || "").trim();
  const fullName = `${firstName}${lastName ? ` ${lastName}` : ""}`.trim();
  const yearsOfExperience = parseNonNegativeInteger(personalInformation?.years_of_experience);
  const positionName = String(posting?.position_name || "the role").trim();
  const companyName = String(posting?.company_name || "your company").trim();
  const linkedinUrl = String(personalInformation?.linkedin_url || "").trim();
  const githubUrl = String(personalInformation?.github_url || "").trim();
  const portfolioUrl = String(personalInformation?.portfolio_url || "").trim();
  const educationLevel = String(personalInformation?.education_level || "").trim();
  const extraInstructions = String(instructions || "").trim();

  const profileDetails = [];
  if (yearsOfExperience > 0) profileDetails.push(`${yearsOfExperience}+ years of relevant experience`);
  if (educationLevel) profileDetails.push(`education in ${educationLevel}`);
  if (linkedinUrl) profileDetails.push(`LinkedIn: ${linkedinUrl}`);
  if (githubUrl) profileDetails.push(`GitHub: ${githubUrl}`);
  if (portfolioUrl) profileDetails.push(`Portfolio: ${portfolioUrl}`);

  const profileSentence =
    profileDetails.length > 0
      ? `My background includes ${profileDetails.join(", ")}.`
      : "I bring hands-on experience delivering high-quality work in fast-moving environments.";

  const instructionSentence = extraInstructions
    ? `I am especially aligned with these priorities: ${extraInstructions}.`
    : "";

  return `Dear Hiring Team,

I am excited to apply for the ${positionName} role at ${companyName}. ${profileSentence}

I am motivated by opportunities where I can contribute quickly, collaborate with a strong team, and improve outcomes for customers and the business. ${instructionSentence}

Thank you for your consideration. I would value the chance to discuss how I can support ${companyName}.

Sincerely,
${fullName}`.trim();
}

async function resolveCompanyIdForApplication(companyName) {
  const normalized = normalizeLikeText(companyName);
  if (!normalized) return null;

  return db.get(
    `
      SELECT id, company_name
      FROM companies
      WHERE LOWER(company_name) = ?
      ORDER BY id ASC
      LIMIT 1;
    `,
    [normalized]
  );
}

async function resolveCompanyIdFromPostingUrl(jobPostingUrl) {
  const normalizedUrl = String(jobPostingUrl || "").trim();
  if (!normalizedUrl) return null;

  const posting = await db.get(
    `
      SELECT company_name
      FROM Postings
      WHERE job_posting_url = ?
      LIMIT 1;
    `,
    [normalizedUrl]
  );

  const normalizedCompanyName = normalizeLikeText(posting?.company_name);
  if (!normalizedCompanyName) return null;

  return db.get(
    `
      SELECT id, company_name
      FROM companies
      WHERE LOWER(company_name) = ?
      ORDER BY id ASC
      LIMIT 1;
    `,
    [normalizedCompanyName]
  );
}

async function getExistingAppliedApplicationByPostingUrl(jobPostingUrl) {
  const normalizedUrl = String(jobPostingUrl || "").trim();
  if (!normalizedUrl) return null;

  const state = await db.get(
    `
      SELECT last_application_id
      FROM posting_application_state
      WHERE job_posting_url = ?
        AND COALESCE(applied, 0) = 1
      LIMIT 1;
    `,
    [normalizedUrl]
  );
  const lastApplicationId = parseNonNegativeInteger(state?.last_application_id);
  if (!lastApplicationId) return null;

  return getApplicationById(lastApplicationId);
}

function mapApplicationRow(row) {
  if (!row) return null;
  const status = normalizeApplicationStatus(row?.status);
  const appliedByType = normalizeAppliedByType(row?.applied_by_type);
  return {
    id: Number(row?.id || 0),
    company_id: Number(row?.company_id || 0),
    company_name: String(row?.company_name || "").trim(),
    position_name: String(row?.position_name || "").trim(),
    application_date: Number(row?.application_date || 0),
    status,
    applied_by_type: appliedByType,
    applied_by_label: normalizeAppliedByLabel(row?.applied_by_label, appliedByType)
  };
}

async function getApplicationById(applicationId) {
  const row = await db.get(
    `
      SELECT
        a.id,
        a.company_id,
        c.company_name,
        a.position_name,
        a.application_date,
        a.status,
        attr.applied_by_type,
        attr.applied_by_label
      FROM applications a
      LEFT JOIN companies c
        ON c.id = a.company_id
      LEFT JOIN application_attribution attr
        ON attr.application_id = a.id
      WHERE a.id = ?;
    `,
    [applicationId]
  );

  return mapApplicationRow(row);
}

async function listApplications(options = {}) {
  const limit = Math.max(1, Math.min(2000, Number(options?.limit || 500)));
  const offset = Math.max(0, Number(options?.offset || 0));
  const status = normalizeLikeText(options?.status);

  let rows = [];
  if (status && status !== "all") {
    rows = await db.all(
      `
        SELECT
          a.id,
          a.company_id,
          c.company_name,
          a.position_name,
          a.application_date,
          a.status,
          attr.applied_by_type,
          attr.applied_by_label
        FROM applications a
        LEFT JOIN companies c
          ON c.id = a.company_id
        LEFT JOIN application_attribution attr
          ON attr.application_id = a.id
        WHERE LOWER(COALESCE(a.status, '')) = ?
        ORDER BY a.application_date DESC, a.id DESC
        LIMIT ? OFFSET ?;
      `,
      [status, limit, offset]
    );
  } else {
    rows = await db.all(
      `
        SELECT
          a.id,
          a.company_id,
          c.company_name,
          a.position_name,
          a.application_date,
          a.status,
          attr.applied_by_type,
          attr.applied_by_label
        FROM applications a
        LEFT JOIN companies c
          ON c.id = a.company_id
        LEFT JOIN application_attribution attr
          ON attr.application_id = a.id
        ORDER BY a.application_date DESC, a.id DESC
        LIMIT ? OFFSET ?;
      `,
      [limit, offset]
    );
  }

  const items = rows.map(mapApplicationRow).filter(Boolean);
  return {
    items,
    count: items.length,
    limit,
    offset
  };
}

async function createApplication(input) {
  const companyName = String(input?.company_name || "").trim();
  const positionName = String(input?.position_name || "").trim();
  const jobPostingUrl = String(input?.job_posting_url || "").trim();
  if (!companyName && !jobPostingUrl) {
    throw new Error("company_name or job_posting_url is required");
  }
  if (!positionName) {
    throw new Error("position_name is required");
  }

  if (jobPostingUrl) {
    const existing = await getExistingAppliedApplicationByPostingUrl(jobPostingUrl);
    if (existing) return existing;
  }

  const companyFromPosting = await resolveCompanyIdFromPostingUrl(jobPostingUrl);
  const company = companyFromPosting || (companyName ? await resolveCompanyIdForApplication(companyName) : null);
  if (!company?.id) {
    throw new Error(
      jobPostingUrl
        ? `Unable to resolve company_id for job_posting_url='${jobPostingUrl}'`
        : `Unable to resolve company_id for company_name='${companyName}'`
    );
  }

  const status = normalizeApplicationStatus(input?.status);
  const applicationDate = parseNonNegativeInteger(input?.application_date) || nowEpochSeconds();
  const appliedByType = normalizeAppliedByType(input?.applied_by_type);
  const appliedByLabel = normalizeAppliedByLabel(input?.applied_by_label, appliedByType);

  await db.exec("BEGIN TRANSACTION;");
  try {
    const result = await db.run(
      `
        INSERT INTO applications (
          company_id,
          position_name,
          application_date,
          status
        ) VALUES (?, ?, ?, ?);
      `,
      [company.id, positionName, applicationDate, status]
    );

    await db.run(
      `
        INSERT INTO application_attribution (
          application_id,
          applied_by_type,
          applied_by_label,
          updated_at
        ) VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(application_id) DO UPDATE SET
          applied_by_type = excluded.applied_by_type,
          applied_by_label = excluded.applied_by_label,
          updated_at = datetime('now');
      `,
      [result.lastID, appliedByType, appliedByLabel]
    );

    if (jobPostingUrl) {
      await markPostingAppliedState({
        job_posting_url: jobPostingUrl,
        applied: true,
        applied_by_type: appliedByType,
        applied_by_label: appliedByLabel,
        applied_at_epoch: applicationDate,
        last_application_id: result.lastID
      });
    }

    await db.exec("COMMIT;");
    return getApplicationById(result.lastID);
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }
}

async function updateApplicationStatus(applicationId, statusValue) {
  const status = normalizeApplicationStatus(statusValue);
  const result = await db.run(
    `
      UPDATE applications
      SET status = ?
      WHERE id = ?;
    `,
    [status, applicationId]
  );

  if (Number(result?.changes || 0) === 0) {
    return null;
  }

  return getApplicationById(applicationId);
}

async function deleteApplicationById(applicationId) {
  await db.exec("BEGIN TRANSACTION;");
  try {
    const trackedPostingRows = await db.all(
      `
        SELECT job_posting_url
        FROM posting_application_state
        WHERE last_application_id = ?;
      `,
      [applicationId]
    );
    const trackedPostingUrls = trackedPostingRows
      .map((row) => String(row?.job_posting_url || "").trim())
      .filter(Boolean);

    await db.run(`DELETE FROM application_attribution WHERE application_id = ?;`, [applicationId]);
    const result = await db.run(`DELETE FROM applications WHERE id = ?;`, [applicationId]);

    for (const jobPostingUrl of trackedPostingUrls) {
      const posting = await db.get(
        `
          SELECT company_name, position_name
          FROM Postings
          WHERE job_posting_url = ?
          LIMIT 1;
        `,
        [jobPostingUrl]
      );

      const companyName = normalizeLikeText(posting?.company_name);
      const positionName = normalizeLikeText(posting?.position_name);

      let replacement = null;
      if (companyName && positionName) {
        replacement = await db.get(
          `
            SELECT
              a.id,
              a.application_date,
              attr.applied_by_type,
              attr.applied_by_label
            FROM applications a
            INNER JOIN companies c
              ON c.id = a.company_id
            LEFT JOIN application_attribution attr
              ON attr.application_id = a.id
            WHERE LOWER(c.company_name) = ?
              AND LOWER(a.position_name) = ?
            ORDER BY a.application_date DESC, a.id DESC
            LIMIT 1;
          `,
          [companyName, positionName]
        );
      }

      if (replacement?.id) {
        const appliedByType = normalizeAppliedByType(replacement?.applied_by_type);
        const appliedByLabel = normalizeAppliedByLabel(replacement?.applied_by_label, appliedByType);
        await db.run(
          `
            UPDATE posting_application_state
            SET
              applied = 1,
              applied_by_type = ?,
              applied_by_label = ?,
              applied_at_epoch = ?,
              last_application_id = ?,
              updated_at = datetime('now')
            WHERE job_posting_url = ?;
          `,
          [
            appliedByType,
            appliedByLabel,
            parseNonNegativeInteger(replacement?.application_date) || nowEpochSeconds(),
            Number(replacement?.id),
            jobPostingUrl
          ]
        );
      } else {
        await db.run(
          `
            UPDATE posting_application_state
            SET
              applied = 0,
              applied_by_type = 'manual',
              applied_by_label = '',
              applied_at_epoch = NULL,
              last_application_id = NULL,
              updated_at = datetime('now')
            WHERE job_posting_url = ?;
          `,
          [jobPostingUrl]
        );
      }
    }

    await db.exec("COMMIT;");
    return Number(result?.changes || 0) > 0;
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }
}

async function getPersonalInformation() {
  const row = await db.get(
    `
      SELECT
        first_name,
        middle_name,
        last_name,
        email,
        phone_number,
        address,
        linkedin_url,
        github_url,
        portfolio_url,
        resume_file_path,
        projects_portfolio_file_path,
        certifications_folder_path,
        ethnicity,
        gender,
        age,
        veteran_status,
        disability_status,
        education_level,
        years_of_experience
      FROM PersonalInformation
      ORDER BY rowid ASC
      LIMIT 1;
    `
  );

  if (!row) {
    return createDefaultPersonalInformation();
  }

  return normalizePersonalInformationInput(row);
}

async function upsertPersonalInformation(value) {
  const normalized = normalizePersonalInformationInput(value);
  const values = PERSONAL_INFORMATION_FIELDS.map((field) => normalized[field]);
  const updateAssignments = PERSONAL_INFORMATION_FIELDS.map((field) => `${field} = ?`).join(", ");
  const existing = await db.get(
    `
      SELECT rowid
      FROM PersonalInformation
      ORDER BY rowid ASC
      LIMIT 1;
    `
  );

  await db.exec("BEGIN TRANSACTION;");
  try {
    if (existing?.rowid) {
      await db.run(
        `
          UPDATE PersonalInformation
          SET ${updateAssignments}
          WHERE rowid = ?;
        `,
        [...values, existing.rowid]
      );

      await db.run(`DELETE FROM PersonalInformation WHERE rowid <> ?;`, [existing.rowid]);
    } else {
      await db.run(
        `
          INSERT INTO PersonalInformation (${PERSONAL_INFORMATION_FIELDS.join(", ")})
          VALUES (${PERSONAL_INFORMATION_FIELDS.map(() => "?").join(", ")});
        `,
        values
      );
    }

    await db.exec("COMMIT;");
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }

  return normalized;
}

async function getCompaniesForSync() {
  return db.all(
    `
      SELECT id, company_name, url_string, ATS_name
      FROM companies
      WHERE LOWER(TRIM(ATS_name)) IN ('workday', 'ashbyhq', 'greenhouseio', 'greenhouse.io', 'greenhouse', 'leverco', 'lever.co', 'lever', 'jobvite', 'jobvite.com', 'jobvitecom', 'applicantpro', 'applicantpro.com', 'applicantprocom', 'applytojob', 'applytojob.com', 'applytojobcom', 'theapplicantmanager', 'theapplicantmanager.com', 'theapplicantmanagercom', 'breezy', 'breezyhr', 'breezy.hr', 'breezyhrcom', 'icims', 'icims.com', 'icimscom', 'zoho', 'zohorecruit', 'zohorecruit.com', 'zohorecruitcom', 'applicantai', 'applicantai.com', 'applicantaicom', 'gem', 'jobs.gem.com', 'gem.com', 'gemcom', 'jobaps', 'jobapscloud.com', 'jobapscloudcom', 'join', 'join.com', 'joincom', 'talentreef', 'jobappnetwork.com', 'jobappnetworkcom', 'apply.jobappnetwork.com', 'applyjobappnetworkcom', 'careerplug', 'careerplug.com', 'careerplugcom', 'bamboohr', 'bamboohr.com', 'bamboohrcom', 'manatal', 'manatal.com', 'manatalcom', 'careers-page.com', 'careerspagecom', 'teamtailor', 'teamtailor.com', 'teamtailorcom', 'careerpuck', 'careerpuck.com', 'careerpuckcom', 'fountain', 'fountain.com', 'fountaincom', 'getro', 'getro.com', 'getrocom', 'hrmdirect', 'hrmdirect.com', 'hrmdirectcom', 'talentlyft', 'talentlyft.com', 'talentlyftcom', 'talexio', 'talexio.com', 'talexiocom', 'saphrcloud', 'saphrcloud.com', 'saphrcloudcom', 'jobs.hr.cloud.sap', 'jobshrcloudsap', 'recruiteecom', 'recruitee.com', 'recruitee', 'ultipro', 'ukg', 'taleo', 'taleo.net', 'taleonet')
        AND NOT EXISTS (
          SELECT 1
          FROM blocked_companies b
          WHERE b.normalized_company_name = LOWER(TRIM(companies.company_name))
        )
      ORDER BY ATS_name ASC, company_name ASC;
    `
  );
}

async function upsertPostings(postings, lastSeenEpoch) {
  if (!Array.isArray(postings) || postings.length === 0) return;
  const seenEpoch = Number(lastSeenEpoch || nowEpochSeconds());

  await db.exec("BEGIN TRANSACTION;");
  try {
    for (const posting of postings) {
      await db.run(
        `
          INSERT OR REPLACE INTO Postings (
            company_name,
            position_name,
            job_posting_url,
            posting_date,
            last_seen_epoch
          )
          VALUES (?, ?, ?, ?, ?);
        `,
        [
          String(posting.company_name || "").trim(),
          String(posting.position_name || "").trim() || "Untitled Position",
          String(posting.job_posting_url || "").trim(),
          posting.posting_date || null,
          seenEpoch
        ]
      );
    }
    await db.exec("COMMIT;");
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }
}

async function pruneExpiredPostings(referenceEpoch = nowEpochSeconds()) {
  const cutoffEpoch = Number(referenceEpoch) - POSTING_TTL_SECONDS;
  const result = await db.run(
    `
      DELETE FROM Postings
      WHERE COALESCE(last_seen_epoch, 0) < ?;
    `,
    [cutoffEpoch]
  );
  return Number(result?.changes || 0);
}

async function prunePostingsOutsideDateWindow(referenceEpoch = nowEpochSeconds()) {
  const rows = await db.all(
    `
      SELECT id, posting_date
      FROM Postings
      WHERE posting_date IS NOT NULL
        AND TRIM(posting_date) <> '';
    `
  );
  if (!Array.isArray(rows) || rows.length === 0) return 0;

  const idsToDelete = [];
  for (const row of rows) {
    const postingId = Number(row?.id || 0);
    if (!Number.isFinite(postingId) || postingId <= 0) continue;
    if (shouldStorePostingByDate(row?.posting_date, referenceEpoch)) continue;
    idsToDelete.push(postingId);
  }

  if (idsToDelete.length === 0) return 0;

  let totalDeleted = 0;
  await db.exec("BEGIN TRANSACTION;");
  try {
    const chunkSize = 800;
    for (let offset = 0; offset < idsToDelete.length; offset += chunkSize) {
      const chunk = idsToDelete.slice(offset, offset + chunkSize);
      if (chunk.length === 0) continue;
      const placeholders = chunk.map(() => "?").join(", ");
      const result = await db.run(
        `
          DELETE FROM Postings
          WHERE id IN (${placeholders});
        `,
        chunk
      );
      totalDeleted += Number(result?.changes || 0);
    }

    await db.exec("COMMIT;");
  } catch (error) {
    await db.exec("ROLLBACK;");
    throw error;
  }

  return totalDeleted;
}

async function runWorkdaySyncInternal() {
  const syncReferenceEpoch = nowEpochSeconds();
  syncStatus.running = true;
  syncStatus.started_at = new Date().toISOString();
  syncStatus.progress = { current: 0, total: 0, company_name: "", total_collected: 0 };
  syncStatus.last_error = null;

  try {
    const companies = await getCompaniesForSync();
    shuffleArrayInPlace(companies);
    syncStatus.progress.total = companies.length;
    let totalPruned = await pruneExpiredPostings(syncReferenceEpoch);
    let postingDatePruned = await prunePostingsOutsideDateWindow(syncReferenceEpoch);
    const nextPostingLocationByJobUrl = new Map();

    const dedupedPostings = new Map();
    const pendingPostingsForUpsert = [];
    const errors = [];
    let excludedByPostingDate = 0;
    let nextCompanyIndex = 0;
    let completedCompanies = 0;
    const workerCount = Math.min(SYNC_WORKER_CONCURRENCY, Math.max(1, companies.length));
    let flushPromise = Promise.resolve();

    const flushPendingPostings = async (force = false) => {
      if (!Array.isArray(pendingPostingsForUpsert) || pendingPostingsForUpsert.length === 0) return;
      if (!force && pendingPostingsForUpsert.length < SYNC_POSTING_FLUSH_BATCH_SIZE) return;

      const batch = pendingPostingsForUpsert.splice(0, pendingPostingsForUpsert.length);
      if (batch.length === 0) return;
      await upsertPostings(batch, syncReferenceEpoch);
    };

    const queueFlushPendingPostings = (force = false) => {
      flushPromise = flushPromise.then(() => flushPendingPostings(force));
      return flushPromise;
    };

    const runSyncWorker = async () => {
      while (true) {
        const currentIndex = nextCompanyIndex;
        if (currentIndex >= companies.length) return;
        nextCompanyIndex += 1;

        const company = companies[currentIndex];
        try {
          const postings = await collectPostingsForCompany(company);
          for (const posting of postings) {
            if (!shouldStorePostingByDate(posting?.posting_date, syncReferenceEpoch)) {
              excludedByPostingDate += 1;
              continue;
            }
            if (dedupedPostings.has(posting.job_posting_url)) continue;
            dedupedPostings.set(posting.job_posting_url, posting);
            pendingPostingsForUpsert.push(posting);
            const location = String(posting?.location || "").trim();
            if (location) {
              nextPostingLocationByJobUrl.set(posting.job_posting_url, location);
              postingLocationByJobUrl.set(posting.job_posting_url, location);
            }
          }
        } catch (error) {
          errors.push({
            company_name: company.company_name,
            message: String(error?.message || error)
          });
        } finally {
          if (pendingPostingsForUpsert.length >= SYNC_POSTING_FLUSH_BATCH_SIZE) {
            await queueFlushPendingPostings(false);
          }
          completedCompanies += 1;
          syncStatus.progress = {
            current: completedCompanies,
            total: companies.length,
            company_name: `${company.company_name} (${company.ATS_name})`,
            total_collected: dedupedPostings.size
          };
        }
      }
    };

    if (companies.length > 0) {
      await Promise.all(Array.from({ length: workerCount }, () => runSyncWorker()));
    }

    await queueFlushPendingPostings(true);

    totalPruned += await pruneExpiredPostings(syncReferenceEpoch);
    postingDatePruned += await prunePostingsOutsideDateWindow(syncReferenceEpoch);
    postingLocationByJobUrl = nextPostingLocationByJobUrl;

    syncStatus.last_sync_at = new Date().toISOString();
    syncStatus.last_sync_summary = {
      total_companies: companies.length,
      total_postings_stored: dedupedPostings.size,
      worker_concurrency: workerCount,
      ats_request_queue_concurrency: atsRequestQueueConcurrency,
      failed_companies: errors.length,
      expired_pruned: totalPruned,
      posting_date_pruned: postingDatePruned,
      excluded_during_sync_by_posting_date: excludedByPostingDate,
      errors: errors.slice(0, 30)
    };
  } catch (error) {
    syncStatus.last_error = String(error?.message || error);
  } finally {
    syncStatus.running = false;
    syncStatus.progress = null;
  }
}

function runWorkdaySync() {
  if (syncPromise) return syncPromise;
  syncPromise = runWorkdaySyncInternal().finally(() => {
    syncPromise = null;
  });
  return syncPromise;
}

async function getCounts() {
  await pruneExpiredPostings();
  const companyRow = await db.get(`SELECT COUNT(*) AS count FROM companies;`);
  const postingRow = await db.get(`SELECT COUNT(*) AS count FROM Postings;`);
  const byAtsRows = await db.all(`
    SELECT ATS_name, COUNT(*) AS count
    FROM companies
    GROUP BY ATS_name;
  `);

  const companyCountByAts = {};
  for (const row of byAtsRows) {
    const key = String(row?.ATS_name || "").trim() || "Unknown";
    companyCountByAts[key] = Number(row?.count || 0);
  }

  return {
    company_count: Number(companyRow?.count || 0),
    posting_count: Number(postingRow?.count || 0),
    company_count_by_ats: companyCountByAts
  };
}

function createServer() {
  const app = express();
  app.use(cors());
  app.use(express.json());

  const handleSyncRequest = async (req, res) => {
    const wait = String(req.query.wait || "").toLowerCase();
    const shouldWait = wait === "1" || wait === "true";
    const wasRunning = Boolean(syncPromise);
    const promise = runWorkdaySync();

    if (shouldWait) {
      await promise;
      const counts = await getCounts();
      return res.json({
        ok: true,
        started: !wasRunning,
        running: syncStatus.running,
        ...syncStatus,
        ...counts
      });
    }

    return res.status(202).json({
      ok: true,
      started: !wasRunning,
      running: true
    });
  };

  app.get("/health", async (_req, res) => {
    const counts = await getCounts();
    res.json({
      ok: true,
      db_path: DB_PATH,
      ...counts
    });
  });

  app.get("/sync/status", async (_req, res) => {
    const counts = await getCounts();
    res.json({
      ...syncStatus,
      ...counts
    });
  });

  app.post("/sync/workday", handleSyncRequest);
  app.post("/sync/ats", handleSyncRequest);

  app.get("/postings/filter-options", async (req, res) => {
    const selectedStates = parseCsvParam(req.query.states).map((state) => state.toUpperCase());
    const ats = [
      { value: "workday", label: "Workday" },
      { value: "ashby", label: "Ashby" },
      { value: "greenhouse", label: "Greenhouse" },
      { value: "lever", label: "Lever" },
      { value: "jobvite", label: "Jobvite" },
      { value: "applicantpro", label: "ApplicantPro" },
      { value: "applytojob", label: "ApplyToJob" },
      { value: "theapplicantmanager", label: "The Applicant Manager" },
      { value: "breezy", label: "BreezyHR" },
      { value: "icims", label: "iCIMS" },
      { value: "zoho", label: "Zoho Recruit" },
      { value: "applicantai", label: "ApplicantAI" },
      { value: "gem", label: "Gem" },
      { value: "jobaps", label: "JobAps" },
      { value: "join", label: "JOIN" },
      { value: "talentreef", label: "TalentReef" },
      { value: "careerplug", label: "CareerPlug" },
      { value: "bamboohr", label: "BambooHR" },
      { value: "manatal", label: "Manatal" },
      { value: "teamtailor", label: "Teamtailor" },
      { value: "careerpuck", label: "CareerPuck" },
      { value: "fountain", label: "Fountain" },
      { value: "getro", label: "Getro" },
      { value: "hrmdirect", label: "HRMDirect" },
      { value: "talentlyft", label: "Talentlyft" },
      { value: "talexio", label: "Talexio" },
      { value: "saphrcloud", label: "SAP HR Cloud" },
      { value: "recruitee", label: "Recruitee" },
      { value: "ultipro", label: "UltiPro" },
      { value: "taleo", label: "Taleo" }
    ];
    const sort_options = [
      { value: "recent", label: "Most Recently Seen" },
      { value: "company_asc", label: "Company (A-Z)" }
    ];

    let industries = [];
    try {
      industries = await db.all(
        `
          SELECT industry_key AS value, industry_label AS label
          FROM job_industry_categories
          ORDER BY industry_label ASC;
        `
      );
    } catch {
      industries = await db.all(
        `
          SELECT industry_key AS value, industry_label AS label
          FROM job_position_industry
          GROUP BY industry_key, industry_label
          ORDER BY industry_label ASC;
        `
      );
    }

    let states = [];
    try {
      const stateRows = await db.all(
        `
          SELECT DISTINCT state_usps
          FROM state_location_index
          WHERE state_usps IS NOT NULL AND TRIM(state_usps) <> ''
          ORDER BY state_usps ASC;
        `
      );
      states = stateRows.map((row) => {
        const code = String(row?.state_usps || "").trim().toUpperCase();
        const readableName = STATE_CODE_TO_NAME[code];
        return {
          value: code,
          label: readableName ? `${code} - ${readableName.replace(/\b\w/g, (c) => c.toUpperCase())}` : code
        };
      });
    } catch {
      states = [];
    }

    let counties = [];
    try {
      let countyRows = [];
      if (selectedStates.length === 0) {
        countyRows = await db.all(
          `
            SELECT DISTINCT state_usps, search_location_name
            FROM state_location_index
            WHERE location_type = 'county'
              AND search_location_name IS NOT NULL
              AND TRIM(search_location_name) <> ''
            ORDER BY state_usps ASC, search_location_name ASC;
          `
        );
      } else {
        const placeholders = selectedStates.map(() => "?").join(", ");
        countyRows = await db.all(
          `
            SELECT DISTINCT state_usps, search_location_name
            FROM state_location_index
            WHERE location_type = 'county'
              AND search_location_name IS NOT NULL
              AND TRIM(search_location_name) <> ''
              AND state_usps IN (${placeholders})
            ORDER BY state_usps ASC, search_location_name ASC;
          `,
          selectedStates
        );
      }

      counties = countyRows.map((row) => {
        const stateCode = String(row?.state_usps || "").trim().toUpperCase();
        const countyName = String(row?.search_location_name || "").trim();
        return {
          value: `${stateCode}|${countyName}`,
          label: `${countyName} (${stateCode})`,
          state: stateCode,
          county: countyName
        };
      });
    } catch {
      counties = [];
    }

    res.json({
      ats,
      sort_options,
      industries,
      states,
      counties
    });
  });

  app.get("/settings/personal-information", async (_req, res) => {
    const item = await getPersonalInformation();
    res.json({ item });
  });

  app.put("/settings/personal-information", async (req, res) => {
    const item = await upsertPersonalInformation(req.body);
    res.json({
      ok: true,
      item
    });
  });

  app.get("/settings/mcp", async (_req, res) => {
    const item = await getMcpSettings();
    res.json({ item });
  });

  app.put("/settings/mcp", async (req, res) => {
    const item = await upsertMcpSettings(req.body || {});
    res.json({
      ok: true,
      item
    });
  });

  app.get("/settings/sync", async (_req, res) => {
    const item = await getSyncServiceSettings();
    res.json({ item });
  });

  app.put("/settings/sync", async (req, res) => {
    const item = await upsertSyncServiceSettings(req.body || {});
    res.json({
      ok: true,
      item
    });
  });

  app.get("/settings/sync/blocked-companies", async (_req, res) => {
    const items = await listBlockedCompanies();
    res.json({
      ok: true,
      items,
      count: items.length
    });
  });

  app.post("/settings/sync/blocked-companies", async (req, res) => {
    try {
      const item = await blockCompanyByName(req.body?.company_name);
      const items = await listBlockedCompanies();
      res.json({
        ok: true,
        item: {
          normalized_company_name: String(item?.normalized_company_name || ""),
          company_name: String(item?.company_name || ""),
          blocked_at_epoch: Number(item?.blocked_at_epoch || 0)
        },
        items,
        count: items.length
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.post("/settings/sync/blocked-companies/unblock", async (req, res) => {
    try {
      const deleted = await unblockCompanyByName(req.body?.company_name);
      const items = await listBlockedCompanies();
      res.json({
        ok: true,
        deleted,
        items,
        count: items.length
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.get("/mcp/candidates", async (req, res) => {
    const settings = await getMcpSettings();
    try {
      ensureMcpAgentEnabled(settings);
    } catch (error) {
      return res.status(Number(error?.statusCode || 403)).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
    const personalInformation = await getPersonalInformation();

    const useSettings = normalizeBoolean(req.query.use_settings, true);
    const overrideSearch = String(req.query.search || "").trim();
    const overrideAts = parseCsvParam(req.query.ats);
    const overrideIndustries = parseCsvParam(req.query.industries);
    const overrideStates = parseCsvParam(req.query.states);
    const overrideCounties = parseCsvParam(req.query.counties);
    const overrideRemote = normalizeRemoteFilter(req.query.remote);
    const includeApplied = normalizeBoolean(req.query.include_applied, false);

    const preferredMax = Math.max(
      1,
      parseNonNegativeInteger(settings?.max_applications_per_run) || MCP_SETTINGS_DEFAULTS.max_applications_per_run
    );
    const requestedLimit = parseNonNegativeInteger(req.query.limit);
    const limit = Math.max(1, Math.min(2000, requestedLimit || preferredMax));

    const search = overrideSearch || (useSettings ? String(settings?.preferred_search || "").trim() : "");
    const ats = overrideAts.length > 0 ? overrideAts : [];
    const industries =
      overrideIndustries.length > 0
        ? overrideIndustries
        : useSettings
          ? normalizeStringArray(settings?.preferred_industries)
          : [];
    const states =
      overrideStates.length > 0
        ? overrideStates
        : useSettings
          ? normalizeStringArray(settings?.preferred_states)
          : [];
    const counties =
      overrideCounties.length > 0
        ? overrideCounties
        : useSettings
          ? normalizeStringArray(settings?.preferred_counties)
          : [];
    const remote = req.query.remote ? overrideRemote : useSettings ? settings?.preferred_remote : "all";

    const result = await listPostingsWithFilters({
      search,
      limit,
      offset: 0,
      ats,
      industries,
      states,
      counties,
      remote,
      include_applied: includeApplied
    });

    const candidates = (result?.items || []).slice(0, limit);
    const runbook = buildMcpRunbook(settings, personalInformation, candidates);

    res.json({
      ok: true,
      count: candidates.length,
      limit,
      filters: result.filters,
      settings,
      personal_information: personalInformation,
      runbook,
      candidates
    });
  });

  app.post("/mcp/cover-letter-draft", async (req, res) => {
    const settings = await getMcpSettings();
    try {
      ensureMcpAgentEnabled(settings);
    } catch (error) {
      return res.status(Number(error?.statusCode || 403)).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
    const personalInformation = await getPersonalInformation();
    const jobPostingUrl = String(req.body?.job_posting_url || "").trim();
    const requestCompanyName = String(req.body?.company_name || "").trim();
    const requestPositionName = String(req.body?.position_name || "").trim();

    let posting = {
      job_posting_url: jobPostingUrl,
      company_name: requestCompanyName,
      position_name: requestPositionName
    };

    if (jobPostingUrl && (!requestCompanyName || !requestPositionName)) {
      const row = await db.get(
        `
          SELECT company_name, position_name, job_posting_url
          FROM Postings
          WHERE job_posting_url = ?
          LIMIT 1;
        `,
        [jobPostingUrl]
      );
      posting = {
        job_posting_url: jobPostingUrl,
        company_name: requestCompanyName || String(row?.company_name || "").trim(),
        position_name: requestPositionName || String(row?.position_name || "").trim()
      };
    }

    const instructions = String(req.body?.instructions || settings?.instructions_for_agent || "").trim();
    const draft = buildCoverLetterDraft(personalInformation, posting, instructions);

    res.json({
      ok: true,
      posting,
      draft
    });
  });

  app.post("/mcp/applications/complete", async (req, res) => {
    try {
      const settings = await getMcpSettings();
      ensureMcpAgentEnabled(settings);
      const commit = normalizeBoolean(req.body?.commit, false);
      const approvedByUser = normalizeBoolean(req.body?.approved_by_user, false);
      const jobPostingUrl = String(req.body?.job_posting_url || "").trim();
      const agentName =
        String(req.body?.agent_name || settings?.preferred_agent_name || MCP_SETTINGS_DEFAULTS.preferred_agent_name)
          .trim() || MCP_SETTINGS_DEFAULTS.preferred_agent_name;

      let companyName = String(req.body?.company_name || "").trim();
      let positionName = String(req.body?.position_name || "").trim();

      if (jobPostingUrl && (!companyName || !positionName)) {
        const posting = await db.get(
          `
            SELECT company_name, position_name
            FROM Postings
            WHERE job_posting_url = ?
            LIMIT 1;
          `,
          [jobPostingUrl]
        );
        companyName = companyName || String(posting?.company_name || "").trim();
        positionName = positionName || String(posting?.position_name || "").trim();
      }

      if (!companyName || !positionName) {
        return res.status(400).json({
          ok: false,
          error: "company_name and position_name are required (or provide a valid job_posting_url)."
        });
      }

      if (commit && settings?.require_final_approval && !approvedByUser) {
        return res.status(400).json({
          ok: false,
          error: "Final approval is required by MCP settings. Set approved_by_user=true to commit."
        });
      }

      const payload = {
        company_name: companyName,
        position_name: positionName,
        job_posting_url: jobPostingUrl,
        application_date: parseNonNegativeInteger(req.body?.application_date) || nowEpochSeconds(),
        status: req.body?.status || "applied",
        applied_by_type: "agent",
        applied_by_label: `${agentName} applied on behalf of user`
      };

      const shouldDryRun = !commit || Boolean(settings?.dry_run_only);
      if (shouldDryRun) {
        return res.json({
          ok: true,
          committed: false,
          dry_run: true,
          payload
        });
      }

      const item = await createApplication(payload);
      return res.status(201).json({
        ok: true,
        committed: true,
        item
      });
    } catch (error) {
      return res.status(Number(error?.statusCode || 400)).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.get("/applications", async (req, res) => {
    const limit = Math.max(1, Math.min(2000, Number(req.query.limit || 500)));
    const offset = Math.max(0, Number(req.query.offset || 0));
    const status = String(req.query.status || "").trim();

    const payload = await listApplications({
      limit,
      offset,
      status
    });

    res.json({
      ...payload,
      status_options: Array.from(APPLICATION_STATUS_OPTIONS)
    });
  });

  app.post("/applications", async (req, res) => {
    try {
      const item = await createApplication(req.body || {});
      res.status(201).json({
        ok: true,
        item
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.patch("/applications/:id", async (req, res) => {
    const applicationId = Number(req.params.id);
    if (!Number.isFinite(applicationId) || applicationId <= 0) {
      return res.status(400).json({
        ok: false,
        error: "application id must be a positive number"
      });
    }

    const item = await updateApplicationStatus(applicationId, req.body?.status);
    if (!item) {
      return res.status(404).json({
        ok: false,
        error: "application not found"
      });
    }

    return res.json({
      ok: true,
      item
    });
  });

  app.delete("/applications/:id", async (req, res) => {
    const applicationId = Number(req.params.id);
    if (!Number.isFinite(applicationId) || applicationId <= 0) {
      return res.status(400).json({
        ok: false,
        error: "application id must be a positive number"
      });
    }

    const deleted = await deleteApplicationById(applicationId);
    if (!deleted) {
      return res.status(404).json({
        ok: false,
        error: "application not found"
      });
    }

    return res.json({
      ok: true,
      deleted: true
    });
  });

  app.post("/postings/ignore", async (req, res) => {
    try {
      const item = await setPostingIgnoredState(req.body || {});
      res.json({
        ok: true,
        item
      });
    } catch (error) {
      res.status(400).json({
        ok: false,
        error: String(error?.message || error)
      });
    }
  });

  app.get("/postings", async (req, res) => {
    const result = await listPostingsWithFilters({
      search: String(req.query.search || "").trim(),
      limit: Number(req.query.limit || 500),
      offset: Number(req.query.offset || 0),
      sort_by: String(req.query.sort_by || "").trim(),
      ats: parseCsvParam(req.query.ats),
      industries: parseCsvParam(req.query.industries),
      states: parseCsvParam(req.query.states),
      counties: parseCsvParam(req.query.counties),
      remote: req.query.remote,
      hide_no_date: normalizeBoolean(req.query.hide_no_date, false),
      include_applied: normalizeBoolean(req.query.include_applied, true),
      include_ignored: normalizeBoolean(req.query.include_ignored, false)
    });

    res.json({
      items: result.items,
      count: result.count,
      limit: result.limit,
      offset: result.offset
    });
  });

  return app;
}

async function start() {
  await initDb();

  const app = createServer();
  app.listen(PORT, () => {
    console.log(`[OpenPostings API] listening on http://localhost:${PORT}`);
    console.log(`[OpenPostings API] using database ${DB_PATH}`);
    console.log(
      `[OpenPostings API] ATS request queue concurrency (runtime): ${atsRequestQueueConcurrency} (saved changes apply after restart)`
    );
  });

  runWorkdaySync().catch((error) => {
    console.error("[OpenPostings API] initial sync failed:", error);
  });

  setInterval(() => {
    runWorkdaySync().catch((error) => {
      console.error("[OpenPostings API] scheduled sync failed:", error);
    });
  }, SYNC_INTERVAL_MS);
}

start().catch((error) => {
  console.error("[OpenPostings API] startup failed:", error);
  process.exit(1);
});
