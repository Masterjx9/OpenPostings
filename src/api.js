import { Platform } from "react-native";

const DEFAULT_API_BASE_URL =
  Platform.OS === "android" ? "http://10.0.2.2:8787" : "http://localhost:8787";

const API_BASE_URL = process.env.EXPO_PUBLIC_API_BASE_URL || DEFAULT_API_BASE_URL;

async function request(path, options = {}) {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }

  return res.json();
}

export function fetchPostings(search = "", limit = 500, offset = 0, filters = {}) {
  const params = new URLSearchParams({
    search,
    limit: String(limit),
    offset: String(offset),
    include_applied: filters?.include_applied === undefined ? "0" : filters?.include_applied ? "1" : "0",
    include_ignored: filters?.include_ignored === undefined ? "0" : filters?.include_ignored ? "1" : "0"
  });

  const atsArray = Array.isArray(filters?.ats) ? filters.ats.filter(Boolean) : [];
  const atsSingle = !Array.isArray(filters?.ats) ? String(filters?.ats || "").trim().toLowerCase() : "";
  const industries = Array.isArray(filters?.industries) ? filters.industries.filter(Boolean) : [];
  const states = Array.isArray(filters?.states) ? filters.states.filter(Boolean) : [];
  const counties = Array.isArray(filters?.counties) ? filters.counties.filter(Boolean) : [];
  const remote = String(filters?.remote || "all").trim().toLowerCase();

  if (atsArray.length > 0) {
    params.set("ats", atsArray.join(","));
  } else if (atsSingle && atsSingle !== "all") {
    params.set("ats", atsSingle);
  }
  if (industries.length > 0) {
    params.set("industries", industries.join(","));
  }
  if (states.length > 0) {
    params.set("states", states.join(","));
  }
  if (counties.length > 0) {
    params.set("counties", counties.join(","));
  }
  if (remote && remote !== "all") {
    params.set("remote", remote);
  }

  return request(`/postings?${params.toString()}`);
}

export function fetchPostingFilterOptions() {
  return request("/postings/filter-options");
}

export function fetchApplications(limit = 500, offset = 0, status = "") {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset)
  });
  if (String(status || "").trim()) {
    params.set("status", String(status).trim());
  }
  return request(`/applications?${params.toString()}`);
}

export function createApplication(payload) {
  return request("/applications", {
    method: "POST",
    body: JSON.stringify(payload || {})
  });
}

export function ignorePosting(payload) {
  return request("/postings/ignore", {
    method: "POST",
    body: JSON.stringify(payload || {})
  });
}

export function updateApplicationStatus(applicationId, status) {
  return request(`/applications/${applicationId}`, {
    method: "PATCH",
    body: JSON.stringify({ status })
  });
}

export function deleteApplication(applicationId) {
  return request(`/applications/${applicationId}`, {
    method: "DELETE"
  });
}

export function fetchSyncStatus() {
  return request("/sync/status");
}

export function triggerWorkdaySync(wait = false) {
  const suffix = wait ? "?wait=1" : "";
  return request(`/sync/ats${suffix}`, { method: "POST" });
}

export function fetchPersonalInformation() {
  return request("/settings/personal-information");
}

export function savePersonalInformation(payload) {
  return request("/settings/personal-information", {
    method: "PUT",
    body: JSON.stringify(payload || {})
  });
}

export function fetchMcpSettings() {
  return request("/settings/mcp");
}

export function saveMcpSettings(payload) {
  return request("/settings/mcp", {
    method: "PUT",
    body: JSON.stringify(payload || {})
  });
}

export function fetchSyncServiceSettings() {
  return request("/settings/sync");
}

export function saveSyncServiceSettings(payload) {
  return request("/settings/sync", {
    method: "PUT",
    body: JSON.stringify(payload || {})
  });
}

export function fetchMcpCandidates(filters = {}) {
  const params = new URLSearchParams();
  const limit = Number(filters?.limit || 0);

  if (filters?.search) params.set("search", String(filters.search).trim());
  if (Array.isArray(filters?.ats) && filters.ats.length > 0) {
    params.set("ats", filters.ats.filter(Boolean).join(","));
  } else if (filters?.ats && String(filters.ats).trim().toLowerCase() !== "all") {
    params.set("ats", String(filters.ats).trim().toLowerCase());
  }
  if (Array.isArray(filters?.industries) && filters.industries.length > 0) {
    params.set("industries", filters.industries.filter(Boolean).join(","));
  }
  if (Array.isArray(filters?.states) && filters.states.length > 0) {
    params.set("states", filters.states.filter(Boolean).join(","));
  }
  if (Array.isArray(filters?.counties) && filters.counties.length > 0) {
    params.set("counties", filters.counties.filter(Boolean).join(","));
  }
  if (filters?.remote) params.set("remote", String(filters.remote));
  if (filters?.include_applied !== undefined) {
    params.set("include_applied", filters.include_applied ? "1" : "0");
  }
  if (filters?.use_settings !== undefined) {
    params.set("use_settings", filters.use_settings ? "1" : "0");
  }
  if (Number.isFinite(limit) && limit > 0) {
    params.set("limit", String(limit));
  }

  const suffix = params.toString() ? `?${params.toString()}` : "";
  return request(`/mcp/candidates${suffix}`);
}

export function fetchMcpCoverLetterDraft(payload) {
  return request("/mcp/cover-letter-draft", {
    method: "POST",
    body: JSON.stringify(payload || {})
  });
}

export function completeMcpApplication(payload) {
  return request("/mcp/applications/complete", {
    method: "POST",
    body: JSON.stringify(payload || {})
  });
}

export { API_BASE_URL };
