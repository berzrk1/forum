import { addRoute, resolve } from "./router.js";
import { renderHeader, mountHeader } from "./components/header.js";
import { renderFooter } from "./components/footer.js";
import { renderHome, mountHome } from "./pages/home.js";
import { renderForum, mountForum } from "./pages/forum.js";
import { renderThread, mountThread } from "./pages/thread.js";
import { renderLogin, mountLogin } from "./pages/login.js";
import { renderRegister, mountRegister } from "./pages/register.js";
import { renderAdmin, mountAdmin } from "./pages/admin.js";
import { isLoggedIn, isTokenExpired, getToken, getUser, parseJwt, setToken, setRole, setUserId, setUser, clearToken } from "./state.js";
import { API_BASE_URL } from "./config.js";

async function refreshTokenIfNeeded() {
  const token = getToken();
  if (!token) return false;

  try {
    const payload = parseJwt(token);
    const now = Math.floor(Date.now() / 1000);
    const expiresIn = payload.exp - now;

    // Refresh if token expires in less than 60 seconds or already expired
    if (expiresIn < 60) {
      const res = await fetch(`${API_BASE_URL}/auth/refresh`, {
        method: "POST",
        credentials: "include",
      });

      if (res.ok) {
        const data = await res.json();
        const newPayload = parseJwt(data.access_token);
        setToken(data.access_token);
        setUserId(newPayload.sub);
        setRole(newPayload.role || "");

        // Fetch username if missing
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
            // Non-critical
          }
        }

        return true;
      } else {
        clearToken();
        return false;
      }
    }
    return true;
  } catch {
    // Token invalid or refresh failed
    clearToken();
    return false;
  }
}

// Register routes — each returns { html, mount? }
addRoute("/", () => ({ html: renderHome(), mount: mountHome }));
addRoute("/forum/:id", ({ id }) => ({ html: renderForum(Number(id)), mount: mountForum }));
addRoute("/thread/:id", ({ id }) => ({ html: renderThread(Number(id)), mount: mountThread }));
addRoute("/login", () => ({ html: renderLogin(), mount: mountLogin }));
addRoute("/register", () => ({ html: renderRegister(), mount: mountRegister }));
addRoute("/admin", () => ({ html: renderAdmin(), mount: mountAdmin }));

function render() {
  const app = document.getElementById("app");
  const page = resolve() || {};
  const pageContent = page.html || notFound();

  app.innerHTML = `
    ${renderHeader()}
    ${pageContent}
    ${renderFooter()}
  `;

  // Attach event listeners after innerHTML is set
  mountHeader();
  if (page.mount) page.mount();

  window.scrollTo(0, 0);
}

function notFound() {
  return `
    <div class="main-content">
      <div class="form-box">
        <div class="form-box__header">404 - Page Not Found</div>
        <div class="form-box__body">
          <p>The page you're looking for doesn't exist.</p>
          <br>
          <a href="#/" class="btn btn--primary">Return to Forum Home</a>
        </div>
      </div>
    </div>
  `;
}

// Listen for hash changes
window.addEventListener("hashchange", render);

// Re-check auth when user returns to tab
document.addEventListener("visibilitychange", async () => {
  if (document.visibilityState === "visible") {
    await refreshTokenIfNeeded();
    render();
  }
});

// Initial render with token refresh check
async function init() {
  await refreshTokenIfNeeded();
  if (!window.location.hash) {
    window.location.hash = "#/";
  } else {
    render();
  }
}

init();
