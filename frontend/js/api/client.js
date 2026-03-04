import { clearToken, setToken, setRole, setUserId, setUser, getUser, parseJwt } from "../state.js";
import { API_BASE_URL } from "../config.js";

let isRefreshing = false;
let refreshPromise = null;

async function refreshAccessToken() {
  const res = await fetch(`${API_BASE_URL}/auth/refresh`, {
    method: "POST",
    credentials: "include", // Send HttpOnly cookie
  });

  if (!res.ok) {
    throw new Error("Refresh failed");
  }

  const data = await res.json();
  const payload = parseJwt(data.access_token);
  setToken(data.access_token);
  setUserId(payload.sub);
  setRole(payload.role || "");

  // Fetch username if not in localStorage
  if (!getUser()) {
    try {
      const meRes = await fetch(`${API_BASE_URL}/users/me`, {
        headers: { "Authorization": `Bearer ${data.access_token}` },
        credentials: "include",
      });
      if (meRes.ok) {
        const me = await meRes.json();
        setUser(me.username);
      }
    } catch {
      // Non-critical, username will be missing from header
    }
  }

  return data.access_token;
}

async function handleTokenRefresh() {
  // Prevent multiple simultaneous refresh attempts
  if (isRefreshing) {
    return refreshPromise;
  }

  isRefreshing = true;
  refreshPromise = refreshAccessToken()
    .finally(() => {
      isRefreshing = false;
      refreshPromise = null;
    });

  return refreshPromise;
}

function handleUnauthorized() {
  clearToken();
  window.location.hash = "#/login";
  window.location.reload();
}

export async function fetchAPI(endpoint, { method = "GET", body } = {}, isRetry = false) {
  const opts = {
    method,
    headers: {},
    credentials: "include", // Always include cookies
    cache: "no-store",
  };

  const token = localStorage.getItem("auth_token");
  if (token) opts.headers["Authorization"] = `Bearer ${token}`;

  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }

  const res = await fetch(`${API_BASE_URL}${endpoint}`, opts);

  if (res.status === 401 && !isRetry) {
    // Try to refresh the token
    try {
      await handleTokenRefresh();
      // Retry the original request with new token
      return fetchAPI(endpoint, { method, body }, true);
    } catch {
      handleUnauthorized();
      throw new Error("Session expired");
    }
  }

  if (res.status === 401) {
    handleUnauthorized();
    throw new Error("Session expired");
  }

  if (res.status === 204) return null;
  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || "Request failed");
  return data;
}

export async function fetchJSON(endpoint, body) {
  const res = await fetch(`${API_BASE_URL}${endpoint}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    credentials: "include", // Include cookies for refresh token
  });

  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || "Request failed");
  return data;
}

export async function fetchForm(endpoint, body) {
  const params = new URLSearchParams(body);
  const res = await fetch(`${API_BASE_URL}${endpoint}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: params,
    credentials: "include", // Include cookies for refresh token
  });

  const data = await res.json();
  if (!res.ok) throw new Error(data.detail || "Request failed");
  return data;
}
