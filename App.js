import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import NetInfo from "@react-native-community/netinfo";
import {
  ActivityIndicator,
  FlatList,
  Image,
  Linking,
  Platform,
  Pressable,
  SafeAreaView,
  ScrollView,
  StyleSheet,
  Switch,
  Text,
  TextInput,
  View
} from "react-native";
import {
  API_BASE_URL,
  createApplication,
  deleteApplication,
  fetchApplications,
  fetchMcpCandidates,
  fetchMcpSettings,
  fetchPostingFilterOptions,
  fetchPersonalInformation,
  fetchPostings,
  fetchSyncServiceSettings,
  fetchSyncStatus,
  ignorePosting,
  saveMcpSettings,
  savePersonalInformation,
  saveSyncServiceSettings,
  triggerWorkdaySync,
  updateApplicationStatus
} from "./src/api";

const PAGE_KEYS = {
  POSTINGS: "postings",
  APPLICATIONS: "applications",
  SETTINGS_APPLICANTEE: "settings_applicantee_information",
  SETTINGS_SYNC: "settings_sync",
  SETTINGS_MCP: "settings_mcp"
};

const PAGE_TITLES = {
  [PAGE_KEYS.POSTINGS]: "Postings",
  [PAGE_KEYS.APPLICATIONS]: "Applications",
  [PAGE_KEYS.SETTINGS_APPLICANTEE]: "Settings / Applicantee Information",
  [PAGE_KEYS.SETTINGS_SYNC]: "Settings / Sync Settings",
  [PAGE_KEYS.SETTINGS_MCP]: "Settings / MCP Settings"
};

const APPLICATION_STATUS_OPTIONS = [
  "applied",
  "interview scheduled",
  "awaiting response",
  "offer received",
  "withdrawn",
  "denied"
];
const DEFAULT_SYNC_INTERVAL_SECONDS = 3600;
const MIN_SYNC_INTERVAL_SECONDS = 60;
const MAX_SYNC_INTERVAL_SECONDS = 24 * 60 * 60;
const DEFAULT_ATS_REQUEST_QUEUE_CONCURRENCY = 1;
const MIN_ATS_REQUEST_QUEUE_CONCURRENCY = 1;
const MAX_ATS_REQUEST_QUEUE_CONCURRENCY = 20;
const DEFAULT_ATS_FILTER_OPTIONS = [
  { value: "workday", label: "Workday" },
  { value: "ashby", label: "Ashby" },
  { value: "greenhouse", label: "Greenhouse" },
  { value: "lever", label: "Lever" },
  { value: "jobvite", label: "Jobvite" },
  { value: "applicantpro", label: "ApplicantPro" },
  { value: "applytojob", label: "ApplyToJob" },
  { value: "theapplicantmanager", label: "The Applicant Manager" },
  { value: "icims", label: "iCIMS" },
  { value: "recruitee", label: "Recruitee" },
  { value: "ultipro", label: "UltiPro" },
  { value: "taleo", label: "Taleo" }
];
const ATS_LABEL_BY_VALUE = {
  workday: "Workday",
  ashby: "Ashby",
  greenhouse: "Greenhouse",
  lever: "Lever",
  jobvite: "Jobvite",
  applicantpro: "ApplicantPro",
  applytojob: "ApplyToJob",
  theapplicantmanager: "The Applicant Manager",
  icims: "iCIMS",
  recruitee: "Recruitee",
  ultipro: "UltiPro",
  taleo: "Taleo"
};

function normalizeAtsValue(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return "";
  if (normalized === "ashbyhq") return "ashby";
  if (normalized === "greenhouseio" || normalized === "greenhouse.io") return "greenhouse";
  if (normalized === "leverco" || normalized === "lever.co") return "lever";
  if (normalized === "jobvitecom" || normalized === "jobvite.com") return "jobvite";
  if (normalized === "applicantprocom" || normalized === "applicantpro.com") return "applicantpro";
  if (normalized === "applytojobcom" || normalized === "applytojob.com") return "applytojob";
  if (normalized === "theapplicantmanagercom" || normalized === "theapplicantmanager.com") {
    return "theapplicantmanager";
  }
  if (normalized === "icimscom" || normalized === "icims.com") return "icims";
  if (normalized === "recruiteecom" || normalized === "recruitee.com") return "recruitee";
  if (normalized === "ukg") return "ultipro";
  if (normalized === "taleonet" || normalized === "taleo.net") return "taleo";
  return normalized;
}

function getAtsDisplayLabel(value) {
  const normalized = normalizeAtsValue(value);
  if (!normalized) return "ATS unavailable";
  return ATS_LABEL_BY_VALUE[normalized] || normalized;
}

function mergeAtsFilterOptions(options) {
  const byValue = new Map();
  const source = Array.isArray(options) ? options : [];

  for (const option of source) {
    const value = normalizeAtsValue(option?.value);
    if (!value) continue;
    const fallbackLabel = getAtsDisplayLabel(value);
    const label = String(option?.label || "").trim() || fallbackLabel;
    byValue.set(value, { value, label });
  }

  for (const option of DEFAULT_ATS_FILTER_OPTIONS) {
    if (!byValue.has(option.value)) {
      byValue.set(option.value, option);
    }
  }

  return Array.from(byValue.values());
}

function normalizeSyncIntervalSeconds(value) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);
  if (!Number.isFinite(parsed)) return DEFAULT_SYNC_INTERVAL_SECONDS;
  if (parsed < MIN_SYNC_INTERVAL_SECONDS) return MIN_SYNC_INTERVAL_SECONDS;
  if (parsed > MAX_SYNC_INTERVAL_SECONDS) return MAX_SYNC_INTERVAL_SECONDS;
  return parsed;
}

function formatSyncIntervalLabel(seconds) {
  const value = normalizeSyncIntervalSeconds(seconds);
  if (value % 3600 === 0) {
    const hours = value / 3600;
    return `${hours} hour${hours === 1 ? "" : "s"}`;
  }
  if (value % 60 === 0) {
    const minutes = value / 60;
    return `${minutes} minute${minutes === 1 ? "" : "s"}`;
  }
  return `${value} seconds`;
}

function normalizeAtsRequestQueueConcurrency(value) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);
  if (!Number.isFinite(parsed)) return DEFAULT_ATS_REQUEST_QUEUE_CONCURRENCY;
  if (parsed < MIN_ATS_REQUEST_QUEUE_CONCURRENCY) return MIN_ATS_REQUEST_QUEUE_CONCURRENCY;
  if (parsed > MAX_ATS_REQUEST_QUEUE_CONCURRENCY) return MAX_ATS_REQUEST_QUEUE_CONCURRENCY;
  return parsed;
}

function createDefaultSyncServiceSettings() {
  return {
    ats_request_queue_concurrency: String(DEFAULT_ATS_REQUEST_QUEUE_CONCURRENCY),
    active_ats_request_queue_concurrency: String(DEFAULT_ATS_REQUEST_QUEUE_CONCURRENCY),
    min_ats_request_queue_concurrency: MIN_ATS_REQUEST_QUEUE_CONCURRENCY,
    max_ats_request_queue_concurrency: MAX_ATS_REQUEST_QUEUE_CONCURRENCY,
    applies_after_service_restart: true
  };
}

function toFormSyncServiceSettings(value) {
  const defaults = createDefaultSyncServiceSettings();
  const source = value && typeof value === "object" ? value : {};
  const configured = normalizeAtsRequestQueueConcurrency(source.ats_request_queue_concurrency);
  const active = normalizeAtsRequestQueueConcurrency(
    source.active_ats_request_queue_concurrency ?? configured
  );
  const minValue = normalizeAtsRequestQueueConcurrency(source.min_ats_request_queue_concurrency || defaults.min_ats_request_queue_concurrency);
  const maxValue = normalizeAtsRequestQueueConcurrency(source.max_ats_request_queue_concurrency || defaults.max_ats_request_queue_concurrency);

  return {
    ats_request_queue_concurrency: String(configured),
    active_ats_request_queue_concurrency: String(active),
    min_ats_request_queue_concurrency: Math.min(minValue, maxValue),
    max_ats_request_queue_concurrency: Math.max(minValue, maxValue),
    applies_after_service_restart: source.applies_after_service_restart !== false
  };
}

const PERSONAL_INFORMATION_FIELDS = [
  { key: "first_name", label: "First Name", placeholder: "Jane", autoCapitalize: "words" },
  { key: "middle_name", label: "Middle Name", placeholder: "Alex", autoCapitalize: "words" },
  { key: "last_name", label: "Last Name", placeholder: "Doe", autoCapitalize: "words" },
  { key: "email", label: "Email", placeholder: "jane@example.com", keyboardType: "email-address" },
  { key: "phone_number", label: "Phone Number", placeholder: "(555) 555-5555", keyboardType: "phone-pad" },
  { key: "address", label: "Address", placeholder: "123 Main St, Seattle, WA", autoCapitalize: "words", multiline: true },
  { key: "linkedin_url", label: "LinkedIn URL", placeholder: "https://linkedin.com/in/username", keyboardType: "url" },
  { key: "github_url", label: "GitHub URL", placeholder: "https://github.com/username", keyboardType: "url" },
  { key: "portfolio_url", label: "Portfolio URL", placeholder: "https://yourportfolio.com", keyboardType: "url" },
  { key: "resume_file_path", label: "Resume File Path", placeholder: "C:\\Users\\You\\Documents\\resume.pdf" },
  { key: "projects_portfolio_file_path", label: "Projects Portfolio File Path", placeholder: "C:\\Users\\You\\Documents\\projects.pdf" },
  { key: "certifications_folder_path", label: "Certifications Folder Path", placeholder: "C:\\Users\\You\\Documents\\certifications" },
  { key: "ethnicity", label: "Ethnicity", placeholder: "Optional value" },
  { key: "gender", label: "Gender", placeholder: "Optional value" },
  { key: "age", label: "Age", placeholder: "29", keyboardType: "numeric" },
  { key: "years_of_experience", label: "Years of Experience", placeholder: "6", keyboardType: "numeric" },
  { key: "veteran_status", label: "Veteran Status", placeholder: "Optional value" },
  { key: "disability_status", label: "Disability Status", placeholder: "Optional value" },
  { key: "education_level", label: "Education Level", placeholder: "Bachelor's Degree" }
];

function createEmptyPersonalInformation() {
  return PERSONAL_INFORMATION_FIELDS.reduce((accumulator, field) => {
    accumulator[field.key] = "";
    return accumulator;
  }, {});
}

function toFormPersonalInformation(value) {
  const source = value && typeof value === "object" ? value : {};
  const formValue = createEmptyPersonalInformation();

  for (const field of PERSONAL_INFORMATION_FIELDS) {
    if (field.key === "age" || field.key === "years_of_experience") {
      const numericValue = source[field.key];
      formValue[field.key] =
        numericValue === null || numericValue === undefined || Number(numericValue) === 0 ? "" : String(numericValue);
      continue;
    }
    formValue[field.key] = String(source[field.key] ?? "");
  }

  return formValue;
}

function createDefaultMcpSettings() {
  return {
    enabled: false,
    preferred_agent_name: "OpenPostings Agent",
    agent_login_email: "",
    agent_login_password: "",
    mfa_login_email: "",
    mfa_login_notes: "",
    dry_run_only: true,
    require_final_approval: true,
    max_applications_per_run: "10",
    preferred_search: "",
    preferred_remote: "all",
    preferred_industries: [],
    preferred_states: [],
    preferred_counties: [],
    instructions_for_agent: ""
  };
}

function toFormMcpSettings(value) {
  const defaults = createDefaultMcpSettings();
  const source = value && typeof value === "object" ? value : {};
  const agentLoginEmail = String(source.agent_login_email || "");
  return {
    ...defaults,
    enabled: Boolean(source.enabled),
    preferred_agent_name: String(source.preferred_agent_name || defaults.preferred_agent_name),
    agent_login_email: agentLoginEmail,
    agent_login_password: String(source.agent_login_password || ""),
    mfa_login_email: agentLoginEmail,
    mfa_login_notes: String(source.mfa_login_notes || ""),
    dry_run_only: source.dry_run_only === undefined ? defaults.dry_run_only : Boolean(source.dry_run_only),
    require_final_approval:
      source.require_final_approval === undefined
        ? defaults.require_final_approval
        : Boolean(source.require_final_approval),
    max_applications_per_run: String(
      source.max_applications_per_run === undefined || source.max_applications_per_run === null
        ? defaults.max_applications_per_run
        : source.max_applications_per_run
    ),
    preferred_search: String(source.preferred_search || ""),
    preferred_remote:
      source.preferred_remote === "remote" || source.preferred_remote === "non_remote" ? source.preferred_remote : "all",
    preferred_industries: Array.isArray(source.preferred_industries) ? source.preferred_industries.filter(Boolean) : [],
    preferred_states: Array.isArray(source.preferred_states) ? source.preferred_states.filter(Boolean) : [],
    preferred_counties: Array.isArray(source.preferred_counties) ? source.preferred_counties.filter(Boolean) : [],
    instructions_for_agent: String(source.instructions_for_agent || "")
  };
}

function toApiMcpSettings(value) {
  const source = value && typeof value === "object" ? value : {};
  const parsedMax = Number.parseInt(String(source.max_applications_per_run || "").trim(), 10);
  const maxApplications = Number.isFinite(parsedMax) && parsedMax > 0 ? parsedMax : 10;
  const agentLoginEmail = String(source.agent_login_email || "").trim();
  return {
    enabled: Boolean(source.enabled),
    preferred_agent_name: String(source.preferred_agent_name || "").trim() || "OpenPostings Agent",
    agent_login_email: agentLoginEmail,
    agent_login_password: String(source.agent_login_password || ""),
    mfa_login_email: agentLoginEmail,
    mfa_login_notes: String(source.mfa_login_notes || "").trim(),
    dry_run_only: Boolean(source.dry_run_only),
    require_final_approval: Boolean(source.require_final_approval),
    max_applications_per_run: maxApplications,
    preferred_search: String(source.preferred_search || "").trim(),
    preferred_remote:
      source.preferred_remote === "remote" || source.preferred_remote === "non_remote" ? source.preferred_remote : "all",
    preferred_industries: Array.isArray(source.preferred_industries) ? source.preferred_industries.filter(Boolean) : [],
    preferred_states: Array.isArray(source.preferred_states) ? source.preferred_states.filter(Boolean) : [],
    preferred_counties: Array.isArray(source.preferred_counties) ? source.preferred_counties.filter(Boolean) : [],
    instructions_for_agent: String(source.instructions_for_agent || "").trim()
  };
}

function PostingCard({ item, onTrackApplication, onIgnorePosting, savingApplicationIds, ignoringPostingIds }) {
  const [menuOpen, setMenuOpen] = useState(false);
  const onOpenPosting = useCallback(async () => {
    const supported = await Linking.canOpenURL(item.job_posting_url);
    if (supported) {
      await Linking.openURL(item.job_posting_url);
    }
  }, [item.job_posting_url]);

  const isSaving = Boolean(savingApplicationIds?.[item.job_posting_url]);
  const isIgnoring = Boolean(ignoringPostingIds?.[item.job_posting_url]);
  const isApplied = Boolean(item?.applied);
  const saveDisabled = isSaving || isApplied || isIgnoring;
  const ignoreDisabled = isIgnoring;
  const atsLabel = getAtsDisplayLabel(item?.ats);

  return (
    <View style={styles.card}>
      <View style={styles.postingCardTopRow}>
        <Pressable onPress={onOpenPosting} style={styles.postingCardMainPressArea}>
          <Text style={styles.position}>{item.position_name}</Text>
          <Text style={styles.location}>{item.location || "Location unavailable"}</Text>
          <Text style={styles.company}>{item.company_name}</Text>
          <Text style={styles.ats}>ATS: {atsLabel}</Text>
          <Text style={styles.posted}>{item.posting_date || "Posting date unavailable"}</Text>
          {isApplied ? (
            <Text style={styles.postingAppliedNotice}>{item.applied_by_label || "Application already tracked"}</Text>
          ) : null}
          <Text numberOfLines={1} style={styles.url}>
            {item.job_posting_url}
          </Text>
        </Pressable>

        <View style={styles.postingCardMenuAnchor}>
          <Pressable
            onPress={() => setMenuOpen((prev) => !prev)}
            style={styles.postingCardMenuTrigger}
          >
            <Text style={styles.postingCardMenuTriggerText}>...</Text>
          </Pressable>

          {menuOpen ? (
            <View style={styles.postingCardMenu}>
              <Pressable
                onPress={() => {
                  setMenuOpen(false);
                  onTrackApplication(item);
                }}
                disabled={saveDisabled}
                style={[styles.postingCardMenuItem, saveDisabled ? styles.postingCardMenuItemDisabled : null]}
              >
                <Text style={styles.postingCardMenuItemText}>
                  {isSaving ? "Saving..." : isApplied ? "Already Applied" : "Save To Applications"}
                </Text>
              </Pressable>

              <Pressable
                onPress={() => {
                  setMenuOpen(false);
                  onIgnorePosting(item);
                }}
                disabled={ignoreDisabled}
                style={[styles.postingCardMenuItem, ignoreDisabled ? styles.postingCardMenuItemDisabled : null]}
              >
                <Text style={styles.postingCardMenuItemText}>{isIgnoring ? "Ignoring..." : "Ignore Job Posting"}</Text>
              </Pressable>
            </View>
          ) : null}
        </View>
      </View>
    </View>
  );
}

function DrawerItem({ label, selected, onPress }) {
  return (
    <Pressable onPress={onPress} style={[styles.drawerItem, selected ? styles.drawerItemSelected : null]}>
      <Text style={[styles.drawerItemText, selected ? styles.drawerItemTextSelected : null]}>{label}</Text>
    </Pressable>
  );
}

function MultiSelectDropdown({
  label,
  options,
  selectedValues,
  onToggleValue,
  onClear,
  emptyText,
  maxVisibleOptions = 80
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const selectedArray = Array.isArray(selectedValues) ? selectedValues : [];
  const normalizedOptions = Array.isArray(options) ? options : [];

  const filteredOptions = useMemo(() => {
    const needle = String(search || "").trim().toLowerCase();
    if (!needle) return normalizedOptions.slice(0, maxVisibleOptions);
    return normalizedOptions
      .filter((option) => String(option?.label || "").toLowerCase().includes(needle))
      .slice(0, maxVisibleOptions);
  }, [maxVisibleOptions, normalizedOptions, search]);

  const selectedCount = selectedArray.length;

  return (
    <View style={styles.dropdownWrap}>
      <Pressable onPress={() => setOpen((prev) => !prev)} style={styles.dropdownTrigger}>
        <Text style={styles.dropdownTriggerLabel}>{label}</Text>
        <Text style={styles.dropdownTriggerValue}>{selectedCount > 0 ? `${selectedCount} selected` : "Any"}</Text>
      </Pressable>

      {open ? (
        <View style={styles.dropdownPanel}>
          <TextInput
            style={styles.dropdownSearch}
            value={search}
            onChangeText={setSearch}
            placeholder={`Search ${label.toLowerCase()}`}
            autoCapitalize="none"
          />

          <ScrollView style={styles.dropdownOptionsScroll}>
            {filteredOptions.length === 0 ? (
              <Text style={styles.dropdownEmpty}>{emptyText || "No matches."}</Text>
            ) : (
              filteredOptions.map((option) => {
                const value = String(option?.value || "");
                const isSelected = selectedArray.includes(value);
                return (
                  <Pressable
                    key={value}
                    onPress={() => onToggleValue(value)}
                    style={[styles.dropdownOption, isSelected ? styles.dropdownOptionSelected : null]}
                  >
                    <Text style={[styles.dropdownOptionLabel, isSelected ? styles.dropdownOptionLabelSelected : null]}>
                      {option?.label}
                    </Text>
                  </Pressable>
                );
              })
            )}
          </ScrollView>

          <Pressable onPress={onClear} style={styles.dropdownClearBtn}>
            <Text style={styles.dropdownClearBtnText}>Clear {label}</Text>
          </Pressable>
        </View>
      ) : null}
    </View>
  );
}

function SingleSelectDropdown({ label, options, selectedValue, onSelectValue, anyLabel = "Any" }) {
  const [open, setOpen] = useState(false);
  const normalizedOptions = Array.isArray(options) ? options : [];
  const selected = String(selectedValue || "all");
  const selectedOption = normalizedOptions.find((option) => String(option?.value || "") === selected);

  return (
    <View style={styles.dropdownWrap}>
      <Pressable onPress={() => setOpen((prev) => !prev)} style={styles.dropdownTrigger}>
        <Text style={styles.dropdownTriggerLabel}>{label}</Text>
        <Text style={styles.dropdownTriggerValue}>{selectedOption?.label || anyLabel}</Text>
      </Pressable>

      {open ? (
        <View style={styles.dropdownPanel}>
          <Pressable
            onPress={() => {
              onSelectValue("all");
              setOpen(false);
            }}
            style={[styles.dropdownOption, selected === "all" ? styles.dropdownOptionSelected : null]}
          >
            <Text style={[styles.dropdownOptionLabel, selected === "all" ? styles.dropdownOptionLabelSelected : null]}>
              {anyLabel}
            </Text>
          </Pressable>

          {normalizedOptions.map((option) => {
            const value = String(option?.value || "");
            const isSelected = selected === value;
            return (
              <Pressable
                key={value}
                onPress={() => {
                  onSelectValue(value || "all");
                  setOpen(false);
                }}
                style={[styles.dropdownOption, isSelected ? styles.dropdownOptionSelected : null]}
              >
                <Text style={[styles.dropdownOptionLabel, isSelected ? styles.dropdownOptionLabelSelected : null]}>
                  {option?.label}
                </Text>
              </Pressable>
            );
          })}
        </View>
      ) : null}
    </View>
  );
}

function ToggleRow({ label, value, onValueChange }) {
  return (
    <View style={styles.toggleRow}>
      <Text style={styles.toggleLabel}>{label}</Text>
      <Switch value={Boolean(value)} onValueChange={onValueChange} />
    </View>
  );
}

export default function App() {
  const [activePage, setActivePage] = useState(PAGE_KEYS.POSTINGS);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [postingsFilters, setPostingsFilters] = useState({
    ats: "all",
    industries: [],
    states: [],
    counties: [],
    remote: "all"
  });
  const [postingFilterOptions, setPostingFilterOptions] = useState({
    ats: DEFAULT_ATS_FILTER_OPTIONS,
    industries: [],
    states: [],
    counties: []
  });
  const [postingFilterOptionsLoading, setPostingFilterOptionsLoading] = useState(false);
  const [postingsFilterPanelOpen, setPostingsFilterPanelOpen] = useState(false);
  const [postings, setPostings] = useState([]);
  const [applications, setApplications] = useState([]);
  const [applicationsLoading, setApplicationsLoading] = useState(false);
  const [applicationsNotice, setApplicationsNotice] = useState("");
  const [savingApplicationIds, setSavingApplicationIds] = useState({});
  const [ignoringPostingIds, setIgnoringPostingIds] = useState({});
  const [updatingApplicationIds, setUpdatingApplicationIds] = useState({});
  const [deletingApplicationIds, setDeletingApplicationIds] = useState({});
  const [openApplicationStatusForId, setOpenApplicationStatusForId] = useState(null);
  const [initializing, setInitializing] = useState(true);
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState(null);
  const [personalInformation, setPersonalInformation] = useState(createEmptyPersonalInformation);
  const [settingsLoading, setSettingsLoading] = useState(false);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsNotice, setSettingsNotice] = useState("");
  const [syncSettings, setSyncSettings] = useState({
    autoSyncEnabled: true,
    wifiOnly: false,
    syncIntervalSeconds: String(DEFAULT_SYNC_INTERVAL_SECONDS)
  });
  const [syncServiceSettings, setSyncServiceSettings] = useState(createDefaultSyncServiceSettings);
  const [syncServiceSettingsLoading, setSyncServiceSettingsLoading] = useState(false);
  const [syncServiceSettingsSaving, setSyncServiceSettingsSaving] = useState(false);
  const [syncSettingsNotice, setSyncSettingsNotice] = useState("");
  const [mcpSettings, setMcpSettings] = useState(createDefaultMcpSettings);
  const [mcpSettingsLoading, setMcpSettingsLoading] = useState(false);
  const [mcpSettingsSaving, setMcpSettingsSaving] = useState(false);
  const [mcpSettingsNotice, setMcpSettingsNotice] = useState("");
  const searchRef = useRef("");
  const postingsFiltersRef = useRef(postingsFilters);
  const autoSyncInFlightRef = useRef(false);

  const pageTitle = PAGE_TITLES[activePage] || PAGE_TITLES[PAGE_KEYS.POSTINGS];
  const remoteFilterOptions = useMemo(
    () => [
      { value: "all", label: "All Locations" },
      { value: "remote", label: "Remote / Hybrid Only" },
      { value: "non_remote", label: "On-Site / Unknown" }
    ],
    []
  );
  const visibleCountyOptions = useMemo(() => {
    const selectedStates = postingsFilters.states || [];
    if (selectedStates.length === 0) return postingFilterOptions.counties || [];
    return (postingFilterOptions.counties || []).filter((county) => selectedStates.includes(county?.state));
  }, [postingFilterOptions.counties, postingsFilters.states]);
  const visibleMcpCountyOptions = useMemo(() => {
    const selectedStates = mcpSettings.preferred_states || [];
    if (selectedStates.length === 0) return postingFilterOptions.counties || [];
    return (postingFilterOptions.counties || []).filter((county) => selectedStates.includes(county?.state));
  }, [mcpSettings.preferred_states, postingFilterOptions.counties]);

  const statusText = useMemo(() => {
    if (!status) return "No sync status yet.";
    const syncTime = status.last_sync_at
      ? new Date(status.last_sync_at).toLocaleString()
      : "No sync has run yet.";
    const summary = status.last_sync_summary || {};
    const base = `Last sync: ${syncTime} | Companies: ${status.company_count || 0} | Stored today: ${status.posting_count || 0} | Failed companies: ${summary.failed_companies || 0}`;
    if (status.running && status.progress) {
      return `${base} | Syncing ${status.progress.current}/${status.progress.total}: ${status.progress.company_name || ""} (collected ${status.progress.total_collected || 0})`;
    }
    return base;
  }, [status]);

  const navigateToPage = useCallback((page) => {
    setActivePage(page);
    setDrawerOpen(false);
  }, []);

  const loadPostings = useCallback(async (q, options = {}) => {
    const silent = Boolean(options.silent);
    const filters = options.filters || postingsFiltersRef.current;
    if (!silent) {
      setLoading(true);
    }
    setError("");
    try {
      const response = await fetchPostings(q, 1000, 0, filters);
      setPostings(response.items || []);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      if (!silent) {
        setLoading(false);
      }
    }
  }, []);

  const loadPostingFilterOptions = useCallback(async () => {
    setPostingFilterOptionsLoading(true);
    try {
      const response = await fetchPostingFilterOptions();
      setPostingFilterOptions({
        ats: mergeAtsFilterOptions(response?.ats),
        industries: Array.isArray(response?.industries) ? response.industries : [],
        states: Array.isArray(response?.states) ? response.states : [],
        counties: Array.isArray(response?.counties) ? response.counties : []
      });
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setPostingFilterOptionsLoading(false);
    }
  }, []);

  const loadApplications = useCallback(async (options = {}) => {
    const silent = Boolean(options.silent);
    if (!silent) {
      setApplicationsLoading(true);
    }
    try {
      const response = await fetchApplications(1000, 0);
      setApplications(response?.items || []);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      if (!silent) {
        setApplicationsLoading(false);
      }
    }
  }, []);

  const loadStatus = useCallback(async () => {
    try {
      const response = await fetchSyncStatus();
      setStatus(response);
      setSyncing(Boolean(response?.running));
      return response;
    } catch (e) {
      setError(String(e.message || e));
      return null;
    }
  }, []);

  const loadPersonalInformation = useCallback(async (options = {}) => {
    const silent = Boolean(options.silent);
    if (!silent) {
      setSettingsLoading(true);
    }
    try {
      const response = await fetchPersonalInformation();
      setPersonalInformation(toFormPersonalInformation(response?.item));
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      if (!silent) {
        setSettingsLoading(false);
      }
    }
  }, []);

  const loadMcpSettings = useCallback(async (options = {}) => {
    const silent = Boolean(options.silent);
    if (!silent) {
      setMcpSettingsLoading(true);
    }
    try {
      const response = await fetchMcpSettings();
      setMcpSettings(toFormMcpSettings(response?.item));
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      if (!silent) {
        setMcpSettingsLoading(false);
      }
    }
  }, []);

  const loadSyncServiceSettings = useCallback(async (options = {}) => {
    const silent = Boolean(options.silent);
    if (!silent) {
      setSyncServiceSettingsLoading(true);
    }
    try {
      const response = await fetchSyncServiceSettings();
      setSyncServiceSettings(toFormSyncServiceSettings(response?.item));
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      if (!silent) {
        setSyncServiceSettingsLoading(false);
      }
    }
  }, []);

  const runSync = useCallback(async () => {
    setError("");
    try {
      await triggerWorkdaySync(false);
      await loadStatus();
    } catch (e) {
      setError(String(e.message || e));
    }
  }, [loadStatus]);

  const handleSaveApplicanteeInformation = useCallback(async () => {
    setError("");
    setSettingsNotice("");
    setSettingsSaving(true);
    try {
      const payload = { ...personalInformation };
      const response = await savePersonalInformation(payload);
      setPersonalInformation(toFormPersonalInformation(response?.item || payload));
      setSettingsNotice("Applicantee information saved.");
    } catch (e) {
      setError(String(e.message || e));
      setSettingsNotice("Unable to save applicantee information.");
    } finally {
      setSettingsSaving(false);
    }
  }, [personalInformation]);

  const handleChangePersonalInformation = useCallback((fieldKey, value) => {
    setPersonalInformation((prev) => ({
      ...prev,
      [fieldKey]: value
    }));
  }, []);

  const handleSaveSyncSettings = useCallback(async () => {
    setError("");
    setSyncSettingsNotice("");
    const syncIntervalSeconds = normalizeSyncIntervalSeconds(syncSettings.syncIntervalSeconds);
    const atsRequestQueueConcurrency = normalizeAtsRequestQueueConcurrency(
      syncServiceSettings.ats_request_queue_concurrency
    );

    setSyncSettings((prev) => ({
      ...prev,
      syncIntervalSeconds: String(syncIntervalSeconds)
    }));
    setSyncServiceSettings((prev) => ({
      ...prev,
      ats_request_queue_concurrency: String(atsRequestQueueConcurrency)
    }));

    const intervalLabel = formatSyncIntervalLabel(syncIntervalSeconds);
    const networkScope =
      Platform.OS === "android"
        ? syncSettings.wifiOnly
          ? "on Wi-Fi only"
          : "on any network"
        : "on any network (Wi-Fi-only applies on Android)";
    const statusLabel = syncSettings.autoSyncEnabled ? `enabled every ${intervalLabel} ${networkScope}` : "disabled";
    const localSavedMessage = `Sync settings saved locally at ${new Date().toLocaleTimeString()}. Auto sync is ${statusLabel}.`;

    setSyncServiceSettingsSaving(true);
    try {
      const response = await saveSyncServiceSettings({
        ats_request_queue_concurrency: atsRequestQueueConcurrency
      });
      const saved = toFormSyncServiceSettings(response?.item);
      setSyncServiceSettings(saved);
      setSyncSettingsNotice(
        `${localSavedMessage} ATS request queue concurrency saved as ${saved.ats_request_queue_concurrency}. This will take effect next time you stop and restart the sync service.`
      );
    } catch (e) {
      setError(String(e.message || e));
      setSyncSettingsNotice(
        `${localSavedMessage} Unable to save ATS request queue concurrency on the server.`
      );
    } finally {
      setSyncServiceSettingsSaving(false);
    }
  }, [syncServiceSettings.ats_request_queue_concurrency, syncSettings]);

  const handleSaveMcpSettings = useCallback(async () => {
    setError("");
    setMcpSettingsNotice("");
    setMcpSettingsSaving(true);
    try {
      const payload = toApiMcpSettings(mcpSettings);
      const response = await saveMcpSettings(payload);
      const savedSettings = toFormMcpSettings(response?.item || payload);
      setMcpSettings(savedSettings);

      const preview = await fetchMcpCandidates({
        use_settings: true,
        include_applied: false,
        limit: Number.parseInt(savedSettings.max_applications_per_run, 10) || 10
      });
      setMcpSettingsNotice(`MCP settings saved. ${preview?.count || 0} candidate postings currently match.`);
    } catch (e) {
      setError(String(e.message || e));
      setMcpSettingsNotice("Unable to save MCP settings.");
    } finally {
      setMcpSettingsSaving(false);
    }
  }, [mcpSettings]);

  const handleTrackPostingApplication = useCallback(
    async (posting) => {
      const postingKey = String(posting?.job_posting_url || "").trim();
      if (!postingKey) return;

      setSavingApplicationIds((prev) => ({
        ...prev,
        [postingKey]: true
      }));
      setError("");
      try {
        await createApplication({
          company_name: posting.company_name,
          position_name: posting.position_name,
          job_posting_url: posting.job_posting_url,
          application_date: Math.floor(Date.now() / 1000),
          status: "applied",
          applied_by_type: "manual",
          applied_by_label: "Manually applied by user"
        });
        setPostings((prev) =>
          prev.map((item) =>
            item.job_posting_url === postingKey
              ? {
                  ...item,
                  applied: true,
                  applied_by_type: "manual",
                  applied_by_label: "Manually applied by user"
                }
              : item
          )
        );
        setApplicationsNotice(`Saved "${posting.position_name}" to Applications.`);
        await loadApplications({ silent: true });
      } catch (e) {
        setError(String(e.message || e));
      } finally {
        setSavingApplicationIds((prev) => ({
          ...prev,
          [postingKey]: false
        }));
      }
    },
    [loadApplications]
  );

  const handleIgnorePosting = useCallback(async (posting) => {
    const postingKey = String(posting?.job_posting_url || "").trim();
    if (!postingKey) return;

    setIgnoringPostingIds((prev) => ({
      ...prev,
      [postingKey]: true
    }));
    setError("");
    try {
      await ignorePosting({
        job_posting_url: posting.job_posting_url,
        ignored: true,
        ignored_by_label: "Ignored by user"
      });
      setPostings((prev) => prev.filter((item) => item.job_posting_url !== postingKey));
      setApplicationsNotice(`Ignored "${posting.position_name}".`);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setIgnoringPostingIds((prev) => ({
        ...prev,
        [postingKey]: false
      }));
    }
  }, []);

  const handleUpdateApplicationStatus = useCallback(async (applicationId, nextStatus) => {
    setUpdatingApplicationIds((prev) => ({
      ...prev,
      [applicationId]: true
    }));
    setError("");
    try {
      const response = await updateApplicationStatus(applicationId, nextStatus);
      const item = response?.item;
      if (item) {
        setApplications((prev) =>
          prev.map((application) => (application.id === applicationId ? { ...application, ...item } : application))
        );
      }
      setApplicationsNotice(`Updated application status to "${nextStatus}".`);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setUpdatingApplicationIds((prev) => ({
        ...prev,
        [applicationId]: false
      }));
      setOpenApplicationStatusForId(null);
    }
  }, []);

  const handleDeleteApplication = useCallback(async (applicationId) => {
    setDeletingApplicationIds((prev) => ({
      ...prev,
      [applicationId]: true
    }));
    setError("");
    try {
      await deleteApplication(applicationId);
      setApplications((prev) => prev.filter((application) => application.id !== applicationId));
      setApplicationsNotice("Application deleted.");
      setOpenApplicationStatusForId(null);
    } catch (e) {
      setError(String(e.message || e));
    } finally {
      setDeletingApplicationIds((prev) => ({
        ...prev,
        [applicationId]: false
      }));
    }
  }, []);

  const setAtsFilter = useCallback((value) => {
    const nextValue = String(value || "all").trim().toLowerCase();
    setPostingsFilters((prev) => ({
      ...prev,
      ats: nextValue || "all"
    }));
  }, []);

  const toggleIndustryFilter = useCallback((value) => {
    setPostingsFilters((prev) => {
      const next = new Set(prev.industries);
      if (next.has(value)) {
        next.delete(value);
      } else {
        next.add(value);
      }
      return {
        ...prev,
        industries: Array.from(next)
      };
    });
  }, []);

  const toggleStateFilter = useCallback((value) => {
    setPostingsFilters((prev) => {
      const nextStates = new Set(prev.states);
      if (nextStates.has(value)) {
        nextStates.delete(value);
      } else {
        nextStates.add(value);
      }

      const nextStateValues = Array.from(nextStates);
      const nextCounties = prev.counties.filter((countyValue) => {
        const [stateCode] = String(countyValue || "").split("|");
        return !stateCode || nextStateValues.includes(stateCode);
      });

      return {
        ...prev,
        states: nextStateValues,
        counties: nextCounties
      };
    });
  }, []);

  const toggleCountyFilter = useCallback((value) => {
    setPostingsFilters((prev) => {
      const next = new Set(prev.counties);
      if (next.has(value)) {
        next.delete(value);
      } else {
        next.add(value);
      }
      return {
        ...prev,
        counties: Array.from(next)
      };
    });
  }, []);

  const clearAllPostingFilters = useCallback(() => {
    setPostingsFilters({
      ats: "all",
      industries: [],
      states: [],
      counties: [],
      remote: "all"
    });
  }, []);

  const toggleMcpIndustryPreference = useCallback((value) => {
    setMcpSettings((prev) => {
      const next = new Set(prev.preferred_industries || []);
      if (next.has(value)) {
        next.delete(value);
      } else {
        next.add(value);
      }
      return {
        ...prev,
        preferred_industries: Array.from(next)
      };
    });
  }, []);

  const toggleMcpStatePreference = useCallback((value) => {
    setMcpSettings((prev) => {
      const nextStates = new Set(prev.preferred_states || []);
      if (nextStates.has(value)) {
        nextStates.delete(value);
      } else {
        nextStates.add(value);
      }

      const nextStateValues = Array.from(nextStates);
      const nextCounties = (prev.preferred_counties || []).filter((countyValue) => {
        const [stateCode] = String(countyValue || "").split("|");
        return !stateCode || nextStateValues.includes(stateCode);
      });

      return {
        ...prev,
        preferred_states: nextStateValues,
        preferred_counties: nextCounties
      };
    });
  }, []);

  const toggleMcpCountyPreference = useCallback((value) => {
    setMcpSettings((prev) => {
      const next = new Set(prev.preferred_counties || []);
      if (next.has(value)) {
        next.delete(value);
      } else {
        next.add(value);
      }
      return {
        ...prev,
        preferred_counties: Array.from(next)
      };
    });
  }, []);

  useEffect(() => {
    searchRef.current = search;
  }, [search]);

  useEffect(() => {
    postingsFiltersRef.current = postingsFilters;
  }, [postingsFilters]);

  useEffect(() => {
    const bootstrap = async () => {
      setInitializing(true);
      setError("");
      try {
        await Promise.all([
          loadPostings("", { filters: postingsFiltersRef.current }),
          loadStatus(),
          loadPersonalInformation(),
          loadSyncServiceSettings(),
          loadMcpSettings(),
          loadPostingFilterOptions(),
          loadApplications()
        ]);
      } catch (e) {
        setError(String(e.message || e));
      } finally {
        setInitializing(false);
      }
    };

    bootstrap();
  }, [
    loadPostings,
    loadStatus,
    loadPersonalInformation,
    loadSyncServiceSettings,
    loadMcpSettings,
    loadPostingFilterOptions,
    loadApplications
  ]);

  useEffect(() => {
    const timer = setTimeout(() => {
      loadPostings(search, { filters: postingsFilters });
    }, 1800);
    return () => clearTimeout(timer);
  }, [search, postingsFilters, loadPostings]);

  useEffect(() => {
    if (!syncSettings.autoSyncEnabled) return undefined;

    const syncIntervalSeconds = normalizeSyncIntervalSeconds(syncSettings.syncIntervalSeconds);
    const syncIntervalMs = syncIntervalSeconds * 1000;

    const id = setInterval(async () => {
      if (autoSyncInFlightRef.current) return;

      if (Platform.OS === "android" && syncSettings.wifiOnly) {
        try {
          const networkState = await NetInfo.fetch();
          const networkType = String(networkState?.type || "").toLowerCase();
          if (networkType !== "wifi") return;
        } catch {
          return;
        }
      }

      autoSyncInFlightRef.current = true;
      try {
        await runSync();
      } finally {
        autoSyncInFlightRef.current = false;
      }
    }, syncIntervalMs);

    return () => clearInterval(id);
  }, [runSync, syncSettings.autoSyncEnabled, syncSettings.syncIntervalSeconds, syncSettings.wifiOnly]);

  useEffect(() => {
    const id = setInterval(async () => {
      const latest = await loadStatus();
      if (latest) {
        await loadPostings(searchRef.current, { silent: true, filters: postingsFiltersRef.current });
      }
    }, 5000);
    return () => clearInterval(id);
  }, [loadPostings, loadStatus]);

  useEffect(() => {
    const id = setInterval(() => {
      loadStatus();
    }, 30000);
    return () => clearInterval(id);
  }, [loadStatus]);

  useEffect(() => {
    if (activePage !== PAGE_KEYS.APPLICATIONS) return;
    loadApplications({ silent: false });
  }, [activePage, loadApplications]);

  const renderPostingsPage = () => (
    <>
      <View style={styles.controls}>
        <TextInput
          style={styles.search}
          value={search}
          onChangeText={setSearch}
          placeholder="Search company or title"
          autoCapitalize="none"
        />
        <Pressable onPress={runSync} style={styles.syncBtn}>
          <Text style={styles.syncBtnText}>{syncing ? "Syncing..." : "Sync Postings"}</Text>
        </Pressable>
      </View>

      <View style={styles.postingsFiltersHeaderRow}>
        <Pressable onPress={() => setPostingsFilterPanelOpen((prev) => !prev)} style={styles.postingsFiltersToggleBtn}>
          <Text style={styles.postingsFiltersToggleText}>
            {postingsFilterPanelOpen ? "Hide Filters" : "Show Filters"}
          </Text>
        </Pressable>
        <Pressable onPress={clearAllPostingFilters} style={styles.postingsFiltersClearBtn}>
          <Text style={styles.postingsFiltersClearText}>Clear</Text>
        </Pressable>
      </View>

      {postingsFilterPanelOpen ? (
        <View style={styles.postingsFiltersPanel}>
          {postingFilterOptionsLoading ? (
            <Text style={styles.small}>Loading filter options...</Text>
          ) : (
            <>
              <SingleSelectDropdown
                label="ATS"
                options={postingFilterOptions.ats}
                selectedValue={postingsFilters.ats}
                onSelectValue={setAtsFilter}
                anyLabel="All ATS"
              />

              <MultiSelectDropdown
                label="Industries"
                options={postingFilterOptions.industries}
                selectedValues={postingsFilters.industries}
                onToggleValue={toggleIndustryFilter}
                onClear={() =>
                  setPostingsFilters((prev) => ({
                    ...prev,
                    industries: []
                  }))
                }
                emptyText="No industries available."
              />

              <MultiSelectDropdown
                label="States"
                options={postingFilterOptions.states}
                selectedValues={postingsFilters.states}
                onToggleValue={toggleStateFilter}
                onClear={() =>
                  setPostingsFilters((prev) => ({
                    ...prev,
                    states: [],
                    counties: []
                  }))
                }
                emptyText="No states available."
              />

              <MultiSelectDropdown
                label="Counties"
                options={visibleCountyOptions}
                selectedValues={postingsFilters.counties}
                onToggleValue={toggleCountyFilter}
                onClear={() =>
                  setPostingsFilters((prev) => ({
                    ...prev,
                    counties: []
                  }))
                }
                emptyText="No counties match selected states."
              />
            </>
          )}

          <View style={styles.remoteFilterGroup}>
            <Text style={styles.fieldLabel}>Remote Filter</Text>
            <View style={styles.remoteFilterChipsRow}>
              {remoteFilterOptions.map((option) => {
                const selected = postingsFilters.remote === option.value;
                return (
                  <Pressable
                    key={option.value}
                    onPress={() =>
                      setPostingsFilters((prev) => ({
                        ...prev,
                        remote: option.value
                      }))
                    }
                    style={[styles.remoteFilterChip, selected ? styles.remoteFilterChipActive : null]}
                  >
                    <Text style={[styles.remoteFilterChipText, selected ? styles.remoteFilterChipTextActive : null]}>
                      {option.label}
                    </Text>
                  </Pressable>
                );
              })}
            </View>
          </View>
        </View>
      ) : null}

      <Text style={styles.status}>{statusText}</Text>
      {loading && !initializing ? <Text style={styles.small}>Refreshing results...</Text> : null}
      {applicationsNotice ? <Text style={styles.inlineNotice}>{applicationsNotice}</Text> : null}

      {initializing && postings.length === 0 ? (
        <ActivityIndicator size="large" style={styles.loader} />
      ) : (
        <FlatList
          data={postings}
          keyExtractor={(item) => item.job_posting_url}
          renderItem={({ item }) => (
            <PostingCard
              item={item}
              onTrackApplication={handleTrackPostingApplication}
              onIgnorePosting={handleIgnorePosting}
              savingApplicationIds={savingApplicationIds}
              ignoringPostingIds={ignoringPostingIds}
            />
          )}
          ListEmptyComponent={<Text style={styles.empty}>No postings found.</Text>}
          contentContainerStyle={styles.list}
        />
      )}
    </>
  );

  const renderApplicationsPage = () => (
    <ScrollView contentContainerStyle={styles.settingsContent}>
      <View style={styles.settingsCard}>
        <Text style={styles.settingsTitle}>Applications</Text>
        <Text style={styles.settingsDescription}>
          Track jobs you applied to. Entries added from Postings are marked as manual applications.
        </Text>

        {applicationsNotice ? <Text style={styles.settingsNotice}>{applicationsNotice}</Text> : null}
        {applicationsLoading ? <ActivityIndicator size="small" style={styles.settingsLoader} /> : null}

        {!applicationsLoading && applications.length === 0 ? (
          <Text style={styles.empty}>No applications tracked yet.</Text>
        ) : null}

        {applications.map((application) => {
          const statusMenuOpen = openApplicationStatusForId === application.id;
          const isUpdatingStatus = Boolean(updatingApplicationIds[application.id]);
          const isDeleting = Boolean(deletingApplicationIds[application.id]);
          const appliedDate = application?.application_date
            ? new Date(Number(application.application_date) * 1000).toLocaleString()
            : "Unknown date";

          return (
            <View key={application.id} style={styles.applicationCard}>
              <Text style={styles.position}>{application.position_name}</Text>
              <Text style={styles.company}>{application.company_name || "Unknown company"}</Text>
              <Text style={styles.posted}>Applied: {appliedDate}</Text>
              <Text style={styles.applicationAttribution}>{application.applied_by_label || "Manually applied by user"}</Text>

              <View style={styles.applicationActionsRow}>
                <View style={styles.applicationStatusWrap}>
                  <Pressable
                    onPress={() => setOpenApplicationStatusForId((prev) => (prev === application.id ? null : application.id))}
                    disabled={isUpdatingStatus}
                    style={styles.applicationStatusBtn}
                  >
                    <Text style={styles.applicationStatusBtnText}>
                      {isUpdatingStatus ? "Updating..." : `Status: ${application.status || "applied"}`}
                    </Text>
                  </Pressable>

                  {statusMenuOpen ? (
                    <View style={styles.applicationStatusMenu}>
                      {APPLICATION_STATUS_OPTIONS.map((status) => (
                        <Pressable
                          key={`${application.id}-${status}`}
                          onPress={() => handleUpdateApplicationStatus(application.id, status)}
                          style={[
                            styles.applicationStatusMenuItem,
                            application.status === status ? styles.applicationStatusMenuItemActive : null
                          ]}
                        >
                          <Text
                            style={[
                              styles.applicationStatusMenuItemText,
                              application.status === status ? styles.applicationStatusMenuItemTextActive : null
                            ]}
                          >
                            {status}
                          </Text>
                        </Pressable>
                      ))}
                    </View>
                  ) : null}
                </View>

                <Pressable
                  onPress={() => handleDeleteApplication(application.id)}
                  disabled={isDeleting}
                  style={[styles.applicationDeleteBtn, isDeleting ? styles.applicationDeleteBtnDisabled : null]}
                >
                  <Text style={styles.applicationDeleteBtnText}>{isDeleting ? "Deleting..." : "Delete"}</Text>
                </Pressable>
              </View>
            </View>
          );
        })}
      </View>
    </ScrollView>
  );

  const renderApplicanteeSettingsPage = () => (
    <ScrollView contentContainerStyle={styles.settingsContent}>
      <View style={styles.settingsCard}>
        <Text style={styles.settingsTitle}>Settings</Text>
        <Text style={styles.settingsSubsection}>Applicantee information</Text>
        <Text style={styles.settingsDescription}>
          Fill out your personal information so it can be reused for applications.
        </Text>

        {settingsLoading ? (
          <ActivityIndicator size="small" style={styles.settingsLoader} />
        ) : (
          <>
            {PERSONAL_INFORMATION_FIELDS.map((field) => (
              <View key={field.key} style={styles.formGroup}>
                <Text style={styles.fieldLabel}>{field.label}</Text>
                <TextInput
                  style={[styles.textField, field.multiline ? styles.textFieldMultiline : null]}
                  value={personalInformation[field.key]}
                  onChangeText={(value) => handleChangePersonalInformation(field.key, value)}
                  placeholder={field.placeholder}
                  autoCapitalize={field.autoCapitalize || "none"}
                  keyboardType={field.keyboardType || "default"}
                  multiline={Boolean(field.multiline)}
                  numberOfLines={field.multiline ? 3 : 1}
                />
              </View>
            ))}

            {settingsNotice ? <Text style={styles.settingsNotice}>{settingsNotice}</Text> : null}

            <Pressable
              onPress={handleSaveApplicanteeInformation}
              disabled={settingsSaving}
              style={[styles.settingsSaveButton, settingsSaving ? styles.settingsSaveButtonDisabled : null]}
            >
              <Text style={styles.settingsSaveButtonText}>
                {settingsSaving ? "Saving..." : "Save Applicantee Information"}
              </Text>
            </Pressable>
          </>
        )}
      </View>
    </ScrollView>
  );

  const renderSyncSettingsPage = () => (
    <ScrollView contentContainerStyle={styles.settingsContent}>
      <View style={styles.settingsCard}>
        <Text style={styles.settingsTitle}>Settings</Text>
        <Text style={styles.settingsSubsection}>Sync Settings</Text>
        <Text style={styles.settingsDescription}>
          Configure automatic posting sync timing. Wi-Fi-only gating applies only on Android.
        </Text>

        <View style={styles.formGroup}>
          <ToggleRow
            label="Enable automatic sync"
            value={syncSettings.autoSyncEnabled}
            onValueChange={(value) =>
              setSyncSettings((prev) => ({
                ...prev,
                autoSyncEnabled: value
              }))
            }
          />
          <ToggleRow
            label="Only sync on Wi-Fi (Android only)"
            value={syncSettings.wifiOnly}
            onValueChange={(value) =>
              setSyncSettings((prev) => ({
                ...prev,
                wifiOnly: value
              }))
            }
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Sync interval (seconds)</Text>
          <TextInput
            style={styles.textField}
            value={syncSettings.syncIntervalSeconds}
            onChangeText={(value) =>
              setSyncSettings((prev) => ({
                ...prev,
                syncIntervalSeconds: value.replace(/[^0-9]/g, "")
              }))
            }
            keyboardType="numeric"
            placeholder={String(DEFAULT_SYNC_INTERVAL_SECONDS)}
          />
          <Text style={styles.settingsInlineHint}>
            Default: {DEFAULT_SYNC_INTERVAL_SECONDS} ({formatSyncIntervalLabel(DEFAULT_SYNC_INTERVAL_SECONDS)}). Minimum:{" "}
            {MIN_SYNC_INTERVAL_SECONDS} seconds.
          </Text>
          {Platform.OS !== "android" ? (
            <Text style={styles.settingsInlineHint}>Wi-Fi-only sync is inactive on web and Windows.</Text>
          ) : null}
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>ATS request queue concurrency</Text>
          <TextInput
            style={styles.textField}
            value={syncServiceSettings.ats_request_queue_concurrency}
            onChangeText={(value) =>
              setSyncServiceSettings((prev) => ({
                ...prev,
                ats_request_queue_concurrency: value.replace(/[^0-9]/g, "")
              }))
            }
            keyboardType="numeric"
            placeholder={String(DEFAULT_ATS_REQUEST_QUEUE_CONCURRENCY)}
          />
          {syncServiceSettingsLoading ? <ActivityIndicator size="small" style={styles.settingsLoader} /> : null}
          <Text style={styles.settingsInlineHint}>
            Range: {syncServiceSettings.min_ats_request_queue_concurrency} to{" "}
            {syncServiceSettings.max_ats_request_queue_concurrency}. Higher values can increase throughput but may cause
            more 429 responses.
          </Text>
          <Text style={styles.settingsInlineHint}>
            Runtime is currently using {syncServiceSettings.active_ats_request_queue_concurrency}. This will take effect
            next time you stop and restart the sync service.
          </Text>
        </View>

        {syncSettingsNotice ? <Text style={styles.settingsNotice}>{syncSettingsNotice}</Text> : null}

        <Pressable
          onPress={handleSaveSyncSettings}
          disabled={syncServiceSettingsSaving}
          style={[styles.settingsSaveButton, syncServiceSettingsSaving ? styles.settingsSaveButtonDisabled : null]}
        >
          <Text style={styles.settingsSaveButtonText}>{syncServiceSettingsSaving ? "Saving..." : "Save Sync Settings"}</Text>
        </Pressable>
      </View>
    </ScrollView>
  );

  const renderMcpSettingsPage = () => (
    <ScrollView contentContainerStyle={styles.settingsContent}>
      <View style={styles.settingsCard}>
        <Text style={styles.settingsTitle}>Settings</Text>
        <Text style={styles.settingsSubsection}>MCP Settings</Text>
        <Text style={styles.settingsDescription}>
          Configure agent behavior, preferences, and a dedicated agent login email/password used for account creation and MFA.
        </Text>

        {mcpSettingsLoading ? <ActivityIndicator size="small" style={styles.settingsLoader} /> : null}

        <View style={styles.formGroup}>
          <ToggleRow
            label="Enable MCP application agent"
            value={mcpSettings.enabled}
            onValueChange={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                enabled: value
              }))
            }
          />
          <ToggleRow
            label="Dry run only (do not submit)"
            value={mcpSettings.dry_run_only}
            onValueChange={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                dry_run_only: value
              }))
            }
          />
          <ToggleRow
            label="Require final user approval"
            value={mcpSettings.require_final_approval}
            onValueChange={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                require_final_approval: value
              }))
            }
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Preferred agent label</Text>
          <TextInput
            style={styles.textField}
            value={mcpSettings.preferred_agent_name}
            onChangeText={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                preferred_agent_name: value
              }))
            }
            placeholder="Codex, Claude, or OpenPostings Agent"
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Agent login email</Text>
          <TextInput
            style={styles.textField}
            value={mcpSettings.agent_login_email}
            onChangeText={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                agent_login_email: value,
                mfa_login_email: value
              }))
            }
            placeholder="agent-login@example.com"
            keyboardType="email-address"
            autoCapitalize="none"
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Agent login password</Text>
          <TextInput
            style={styles.textField}
            value={mcpSettings.agent_login_password}
            onChangeText={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                agent_login_password: value
              }))
            }
            placeholder="Enter agent inbox password"
            autoCapitalize="none"
            autoCorrect={false}
            secureTextEntry
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>MFA/login notes</Text>
          <TextInput
            style={[styles.textField, styles.textFieldMultiline]}
            value={mcpSettings.mfa_login_notes}
            onChangeText={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                mfa_login_notes: value
              }))
            }
            multiline
            numberOfLines={3}
            placeholder="Example: use auth app first, fallback to backup email"
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Max applications per run</Text>
          <TextInput
            style={styles.textField}
            value={mcpSettings.max_applications_per_run}
            onChangeText={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                max_applications_per_run: value
              }))
            }
            keyboardType="numeric"
            placeholder="10"
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Preferred search text</Text>
          <TextInput
            style={styles.textField}
            value={mcpSettings.preferred_search}
            onChangeText={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                preferred_search: value
              }))
            }
            placeholder="software engineer"
            autoCapitalize="none"
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Preferred remote filter</Text>
          <View style={styles.remoteFilterChipsRow}>
            {remoteFilterOptions.map((option) => {
              const selected = mcpSettings.preferred_remote === option.value;
              return (
                <Pressable
                  key={`mcp-${option.value}`}
                  onPress={() =>
                    setMcpSettings((prev) => ({
                      ...prev,
                      preferred_remote: option.value
                    }))
                  }
                  style={[styles.remoteFilterChip, selected ? styles.remoteFilterChipActive : null]}
                >
                  <Text style={[styles.remoteFilterChipText, selected ? styles.remoteFilterChipTextActive : null]}>
                    {option.label}
                  </Text>
                </Pressable>
              );
            })}
          </View>
        </View>

        <View style={styles.formGroup}>
          <MultiSelectDropdown
            label="Preferred Industries"
            options={postingFilterOptions.industries}
            selectedValues={mcpSettings.preferred_industries}
            onToggleValue={toggleMcpIndustryPreference}
            onClear={() =>
              setMcpSettings((prev) => ({
                ...prev,
                preferred_industries: []
              }))
            }
            emptyText="No industries available."
          />

          <MultiSelectDropdown
            label="Preferred States"
            options={postingFilterOptions.states}
            selectedValues={mcpSettings.preferred_states}
            onToggleValue={toggleMcpStatePreference}
            onClear={() =>
              setMcpSettings((prev) => ({
                ...prev,
                preferred_states: [],
                preferred_counties: []
              }))
            }
            emptyText="No states available."
          />

          <MultiSelectDropdown
            label="Preferred Counties"
            options={visibleMcpCountyOptions}
            selectedValues={mcpSettings.preferred_counties}
            onToggleValue={toggleMcpCountyPreference}
            onClear={() =>
              setMcpSettings((prev) => ({
                ...prev,
                preferred_counties: []
              }))
            }
            emptyText="No counties match selected states."
          />
        </View>

        <View style={styles.formGroup}>
          <Text style={styles.fieldLabel}>Agent instructions</Text>
          <TextInput
            style={[styles.textField, styles.textFieldMultiline]}
            value={mcpSettings.instructions_for_agent}
            onChangeText={(value) =>
              setMcpSettings((prev) => ({
                ...prev,
                instructions_for_agent: value
              }))
            }
            multiline
            numberOfLines={4}
            placeholder="Example: prioritize mid-size companies and skip relocation-only roles."
          />
        </View>

        {mcpSettingsNotice ? <Text style={styles.settingsNotice}>{mcpSettingsNotice}</Text> : null}

        <Pressable
          onPress={handleSaveMcpSettings}
          disabled={mcpSettingsSaving}
          style={[styles.settingsSaveButton, mcpSettingsSaving ? styles.settingsSaveButtonDisabled : null]}
        >
          <Text style={styles.settingsSaveButtonText}>{mcpSettingsSaving ? "Saving..." : "Save MCP Settings"}</Text>
        </Pressable>
      </View>
    </ScrollView>
  );

  const renderActivePage = () => {
    if (activePage === PAGE_KEYS.APPLICATIONS) return renderApplicationsPage();
    if (activePage === PAGE_KEYS.SETTINGS_APPLICANTEE) return renderApplicanteeSettingsPage();
    if (activePage === PAGE_KEYS.SETTINGS_SYNC) return renderSyncSettingsPage();
    if (activePage === PAGE_KEYS.SETTINGS_MCP) return renderMcpSettingsPage();
    return renderPostingsPage();
  };

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.header}>
        <View style={styles.headerTopRow}>
          <Pressable
            onPress={() => setDrawerOpen((prev) => !prev)}
            style={styles.hamburgerButton}
            accessibilityRole="button"
            accessibilityLabel="Open navigation menu"
          >
            <Text style={styles.hamburgerIcon}>{"\u2630"}</Text>
          </Pressable>
          <View style={styles.headerLogoContainer}>
            {activePage === PAGE_KEYS.POSTINGS ? (
              <Image source={require("./logo.png")} style={styles.headerLogo} resizeMode="contain" />
            ) : (
              <Text style={styles.title}>OpenPostings</Text>
            )}
          </View>
        </View>
        <View style={styles.headerTextContainer}>
          <Text style={styles.subtitle}>ATS postings ({Platform.OS})</Text>
          <Text style={styles.small}>API: {API_BASE_URL}</Text>
        </View>
        <Text style={styles.pageTitle}>{pageTitle}</Text>
      </View>

      {error ? <Text style={styles.error}>{error}</Text> : null}
      {renderActivePage()}

      {drawerOpen ? (
        <View style={styles.drawerOverlay}>
          <Pressable style={styles.drawerBackdrop} onPress={() => setDrawerOpen(false)} />
          <View style={styles.drawerPanel}>
            <Text style={styles.drawerHeading}>Navigation</Text>
            <DrawerItem
              label="Postings"
              selected={activePage === PAGE_KEYS.POSTINGS}
              onPress={() => navigateToPage(PAGE_KEYS.POSTINGS)}
            />
            <DrawerItem
              label="Applications"
              selected={activePage === PAGE_KEYS.APPLICATIONS}
              onPress={() => navigateToPage(PAGE_KEYS.APPLICATIONS)}
            />

            <Text style={styles.drawerHeading}>Settings</Text>
            <DrawerItem
              label="Applicantee Information"
              selected={activePage === PAGE_KEYS.SETTINGS_APPLICANTEE}
              onPress={() => navigateToPage(PAGE_KEYS.SETTINGS_APPLICANTEE)}
            />
            <DrawerItem
              label="Sync Settings"
              selected={activePage === PAGE_KEYS.SETTINGS_SYNC}
              onPress={() => navigateToPage(PAGE_KEYS.SETTINGS_SYNC)}
            />
            <DrawerItem
              label="MCP Settings"
              selected={activePage === PAGE_KEYS.SETTINGS_MCP}
              onPress={() => navigateToPage(PAGE_KEYS.SETTINGS_MCP)}
            />
          </View>
        </View>
      ) : null}
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#f4f6f8"
  },
  header: {
    paddingHorizontal: 16,
    paddingTop: 12,
    paddingBottom: 6
  },
  headerTopRow: {
    width: "100%",
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between"
  },
  headerTextContainer: {
    alignItems: "flex-start",
    marginTop: 6
  },
  headerLogoContainer: {
    marginLeft: "auto",
    flexShrink: 0,
    alignItems: "flex-end"
  },
  headerLogo: {
    width: 220,
    height: 52,
    marginTop: 2,
    alignSelf: "flex-end"
  },
  hamburgerButton: {
    width: 40,
    height: 40,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#d3dbe4",
    backgroundColor: "#ffffff",
    justifyContent: "center",
    alignItems: "center",
    marginRight: 10,
    marginTop: 2
  },
  hamburgerIcon: {
    fontSize: 20,
    fontWeight: "700",
    color: "#102a43"
  },
  title: {
    fontSize: 28,
    fontWeight: "700",
    color: "#14213d"
  },
  subtitle: {
    fontSize: 14,
    color: "#4f5d75",
    marginTop: 4
  },
  pageTitle: {
    marginTop: 10,
    fontSize: 13,
    color: "#334e68",
    fontWeight: "600"
  },
  small: {
    fontSize: 11,
    color: "#7a8798",
    marginTop: 2
  },
  controls: {
    flexDirection: "row",
    gap: 8,
    paddingHorizontal: 16,
    paddingVertical: 10
  },
  postingsFiltersHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 16,
    paddingBottom: 6
  },
  postingsFiltersToggleBtn: {
    borderWidth: 1,
    borderColor: "#c6ceda",
    backgroundColor: "#ffffff",
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 8
  },
  postingsFiltersToggleText: {
    color: "#334e68",
    fontWeight: "600",
    fontSize: 12
  },
  postingsFiltersClearBtn: {
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 10,
    paddingHorizontal: 10,
    paddingVertical: 8,
    backgroundColor: "#ffffff"
  },
  postingsFiltersClearText: {
    color: "#7a8798",
    fontSize: 12,
    fontWeight: "600"
  },
  postingsFiltersPanel: {
    marginHorizontal: 16,
    marginBottom: 8,
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 12,
    backgroundColor: "#ffffff",
    padding: 10
  },
  dropdownWrap: {
    marginBottom: 10
  },
  dropdownTrigger: {
    borderWidth: 1,
    borderColor: "#c6ceda",
    borderRadius: 10,
    backgroundColor: "#f8fafc",
    paddingHorizontal: 12,
    paddingVertical: 10,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center"
  },
  dropdownTriggerLabel: {
    fontSize: 12,
    fontWeight: "700",
    color: "#334e68"
  },
  dropdownTriggerValue: {
    fontSize: 12,
    color: "#52606d",
    fontWeight: "600"
  },
  dropdownPanel: {
    marginTop: 8,
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 10,
    backgroundColor: "#ffffff",
    padding: 8
  },
  dropdownSearch: {
    borderWidth: 1,
    borderColor: "#c6ceda",
    borderRadius: 10,
    backgroundColor: "#ffffff",
    height: 40,
    paddingHorizontal: 10
  },
  dropdownOptionsScroll: {
    maxHeight: 180,
    marginTop: 8
  },
  dropdownOption: {
    borderWidth: 1,
    borderColor: "#edf2f7",
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 8,
    marginBottom: 6,
    backgroundColor: "#f8fafc"
  },
  dropdownOptionSelected: {
    borderColor: "#0b6e4f",
    backgroundColor: "#e8f6ef"
  },
  dropdownOptionLabel: {
    color: "#334e68",
    fontSize: 12
  },
  dropdownOptionLabelSelected: {
    color: "#0b6e4f",
    fontWeight: "700"
  },
  dropdownEmpty: {
    color: "#7a8798",
    fontSize: 12,
    paddingVertical: 8,
    paddingHorizontal: 4
  },
  dropdownClearBtn: {
    marginTop: 4,
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 8,
    paddingVertical: 8,
    alignItems: "center",
    backgroundColor: "#ffffff"
  },
  dropdownClearBtnText: {
    color: "#52606d",
    fontSize: 12,
    fontWeight: "600"
  },
  remoteFilterGroup: {
    marginTop: 2
  },
  remoteFilterChipsRow: {
    flexDirection: "row",
    gap: 8,
    flexWrap: "wrap"
  },
  remoteFilterChip: {
    borderWidth: 1,
    borderColor: "#c6ceda",
    backgroundColor: "#ffffff",
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 8
  },
  remoteFilterChipActive: {
    borderColor: "#102a43",
    backgroundColor: "#102a43"
  },
  remoteFilterChipText: {
    color: "#334e68",
    fontSize: 12,
    fontWeight: "600"
  },
  remoteFilterChipTextActive: {
    color: "#ffffff"
  },
  search: {
    flex: 1,
    borderWidth: 1,
    borderColor: "#c6ceda",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    height: 42
  },
  syncBtn: {
    backgroundColor: "#0b6e4f",
    borderRadius: 10,
    paddingHorizontal: 14,
    justifyContent: "center"
  },
  syncBtnText: {
    color: "#fff",
    fontWeight: "600"
  },
  status: {
    paddingHorizontal: 16,
    fontSize: 12,
    color: "#334e68"
  },
  error: {
    marginHorizontal: 16,
    marginTop: 2,
    color: "#b00020",
    fontSize: 13
  },
  loader: {
    marginTop: 20
  },
  list: {
    padding: 12,
    gap: 10
  },
  card: {
    backgroundColor: "#fff",
    borderRadius: 12,
    padding: 12,
    borderWidth: 1,
    borderColor: "#dbe2ea"
  },
  position: {
    fontSize: 16,
    fontWeight: "600",
    color: "#102a43"
  },
  location: {
    marginTop: 4,
    fontSize: 12,
    color: "#486581"
  },
  company: {
    marginTop: 4,
    fontSize: 14,
    color: "#334e68"
  },
  ats: {
    marginTop: 3,
    fontSize: 12,
    color: "#243b53",
    fontWeight: "600"
  },
  posted: {
    marginTop: 2,
    fontSize: 12,
    color: "#486581"
  },
  postingAppliedNotice: {
    marginTop: 6,
    fontSize: 12,
    color: "#0b6e4f",
    fontWeight: "600"
  },
  url: {
    marginTop: 6,
    fontSize: 11,
    color: "#7b8794"
  },
  postingCardTopRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    gap: 8
  },
  postingCardMainPressArea: {
    flex: 1,
    minWidth: 0
  },
  postingCardMenuAnchor: {
    position: "relative",
    zIndex: 2
  },
  postingCardMenuTrigger: {
    borderWidth: 1,
    borderColor: "#c6ceda",
    borderRadius: 8,
    minWidth: 34,
    height: 30,
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "#ffffff"
  },
  postingCardMenuTriggerText: {
    fontSize: 18,
    lineHeight: 20,
    color: "#334e68",
    fontWeight: "700"
  },
  postingCardMenu: {
    position: "absolute",
    top: 34,
    right: 0,
    minWidth: 190,
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 10,
    backgroundColor: "#ffffff",
    padding: 6
  },
  postingCardMenuItem: {
    borderWidth: 1,
    borderColor: "#edf2f7",
    borderRadius: 8,
    paddingVertical: 8,
    paddingHorizontal: 10,
    marginBottom: 6,
    backgroundColor: "#f8fafc"
  },
  postingCardMenuItemDisabled: {
    opacity: 0.6
  },
  postingCardMenuItemText: {
    color: "#334e68",
    fontWeight: "600",
    fontSize: 12
  },
  postingCardActionSaveDisabled: {
    opacity: 0.65
  },
  inlineNotice: {
    paddingHorizontal: 16,
    marginTop: 4,
    color: "#0b6e4f",
    fontSize: 12
  },
  empty: {
    textAlign: "center",
    marginTop: 20,
    color: "#52606d"
  },
  applicationCard: {
    marginTop: 12,
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 10,
    padding: 10,
    backgroundColor: "#fdfefe"
  },
  applicationAttribution: {
    marginTop: 4,
    fontSize: 12,
    color: "#334e68",
    fontStyle: "italic"
  },
  applicationActionsRow: {
    marginTop: 10,
    flexDirection: "row",
    gap: 8
  },
  applicationStatusWrap: {
    flex: 1
  },
  applicationStatusBtn: {
    borderWidth: 1,
    borderColor: "#c6ceda",
    borderRadius: 8,
    backgroundColor: "#ffffff",
    paddingVertical: 8,
    paddingHorizontal: 10
  },
  applicationStatusBtnText: {
    color: "#334e68",
    fontSize: 12,
    fontWeight: "600"
  },
  applicationStatusMenu: {
    marginTop: 6,
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 8,
    backgroundColor: "#ffffff",
    padding: 6
  },
  applicationStatusMenuItem: {
    borderWidth: 1,
    borderColor: "#edf2f7",
    borderRadius: 8,
    paddingVertical: 7,
    paddingHorizontal: 8,
    marginBottom: 6,
    backgroundColor: "#f8fafc"
  },
  applicationStatusMenuItemActive: {
    borderColor: "#102a43",
    backgroundColor: "#102a43"
  },
  applicationStatusMenuItemText: {
    color: "#334e68",
    fontSize: 12
  },
  applicationStatusMenuItemTextActive: {
    color: "#ffffff",
    fontWeight: "700"
  },
  applicationDeleteBtn: {
    borderWidth: 1,
    borderColor: "#d13a3a",
    borderRadius: 8,
    backgroundColor: "#d13a3a",
    justifyContent: "center",
    alignItems: "center",
    paddingHorizontal: 10,
    minWidth: 84
  },
  applicationDeleteBtnDisabled: {
    opacity: 0.65
  },
  applicationDeleteBtnText: {
    color: "#ffffff",
    fontWeight: "700",
    fontSize: 12
  },
  settingsContent: {
    paddingHorizontal: 12,
    paddingBottom: 24
  },
  settingsCard: {
    backgroundColor: "#ffffff",
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 12,
    padding: 12
  },
  settingsTitle: {
    fontSize: 18,
    fontWeight: "700",
    color: "#102a43"
  },
  settingsSubsection: {
    marginTop: 4,
    fontSize: 14,
    fontWeight: "600",
    color: "#334e68"
  },
  settingsDescription: {
    marginTop: 6,
    fontSize: 12,
    color: "#52606d"
  },
  settingsLoader: {
    marginTop: 12
  },
  formGroup: {
    marginTop: 12
  },
  fieldLabel: {
    marginBottom: 6,
    fontSize: 12,
    fontWeight: "600",
    color: "#334e68"
  },
  textField: {
    borderWidth: 1,
    borderColor: "#c6ceda",
    borderRadius: 10,
    backgroundColor: "#fff",
    paddingHorizontal: 12,
    height: 42
  },
  textFieldMultiline: {
    minHeight: 72,
    paddingTop: 10,
    paddingBottom: 10,
    textAlignVertical: "top"
  },
  toggleRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 10,
    backgroundColor: "#f8fafc",
    paddingVertical: 10,
    paddingHorizontal: 12,
    marginBottom: 8
  },
  toggleLabel: {
    flex: 1,
    marginRight: 10,
    fontSize: 12,
    color: "#334e68",
    fontWeight: "600"
  },
  settingsNotice: {
    marginTop: 12,
    fontSize: 12,
    color: "#0b6e4f"
  },
  settingsInlineHint: {
    marginTop: 6,
    fontSize: 11,
    color: "#52606d"
  },
  settingsSaveButton: {
    marginTop: 10,
    backgroundColor: "#0b6e4f",
    borderRadius: 10,
    paddingVertical: 12,
    alignItems: "center"
  },
  settingsSaveButtonDisabled: {
    opacity: 0.65
  },
  settingsSaveButtonText: {
    color: "#ffffff",
    fontWeight: "600"
  },
  drawerOverlay: {
    ...StyleSheet.absoluteFillObject,
    zIndex: 20,
    flexDirection: "row"
  },
  drawerBackdrop: {
    flex: 1,
    backgroundColor: "rgba(16, 42, 67, 0.25)"
  },
  drawerPanel: {
    position: "absolute",
    left: 0,
    top: 0,
    bottom: 0,
    width: 286,
    backgroundColor: "#ffffff",
    borderRightWidth: 1,
    borderRightColor: "#dbe2ea",
    paddingTop: 58,
    paddingHorizontal: 12
  },
  drawerHeading: {
    marginTop: 12,
    marginBottom: 6,
    paddingHorizontal: 8,
    fontSize: 12,
    color: "#7a8798",
    textTransform: "uppercase",
    fontWeight: "700"
  },
  drawerItem: {
    borderWidth: 1,
    borderColor: "#dbe2ea",
    borderRadius: 10,
    paddingVertical: 10,
    paddingHorizontal: 10,
    marginBottom: 8
  },
  drawerItemSelected: {
    borderColor: "#102a43",
    backgroundColor: "#102a43"
  },
  drawerItemText: {
    color: "#334e68",
    fontWeight: "600"
  },
  drawerItemTextSelected: {
    color: "#ffffff"
  }
});
