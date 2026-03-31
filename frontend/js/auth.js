const redirectUri = window.location.origin;

function getAuthConfig() {
  const config = window.APP_CONFIG || {};

  const clientId = config.AZURE_AD_CLIENT_ID;
  const tenantId = config.AZURE_AD_TENANT_ID;
  const apiAudience = config.AZURE_AD_API_AUDIENCE;

  if (!clientId || !tenantId || !apiAudience) {
    throw new Error(
      "Missing Azure AD config. Check AZURE_AD_CLIENT_ID, AZURE_AD_TENANT_ID and AZURE_AD_API_AUDIENCE."
    );
  }

  return {
    clientId,
    authority: config.AZURE_AD_AUTHORITY || `https://login.microsoftonline.com/${tenantId}`,
    apiScope: config.AZURE_AD_API_SCOPE || `${apiAudience}/access_as_user`
  };
}

const authConfig = getAuthConfig();


const msalConfig = {
  auth: {
    clientId: authConfig.clientId,
    authority: authConfig.authority,
    redirectUri
  }
};

const msalInstance = new msal.PublicClientApplication(msalConfig);

async function login() {
  const loginResponse = await msalInstance.loginPopup({
    scopes: ["openid", "profile"]
  });

  sessionStorage.setItem("id_token", loginResponse.idToken);
  log("Login successful. Authentication token stored.");
  msalInstance.setActiveAccount(loginResponse.account);
}

async function logout() {
  // pulizia stato locale
  msalInstance.setActiveAccount(null);
  sessionStorage.removeItem("id_token");

  log("User logged out.");

  // reload app
  window.location.reload();
}

async function getAccessToken() {
  const account = msalInstance.getActiveAccount();

  if (!account) {
    throw new Error("No active account");
  }

  const response = await msalInstance.acquireTokenSilent({
    scopes: [authConfig.apiScope],
    account: account
  });

  return response.accessToken;
}

async function apiFetch(url, options = {}) {
  const accessToken = await getAccessToken();

  const headers = {
    ...options.headers,
    Authorization: `Bearer ${accessToken}`,
  };

  // setta JSON solo se NON è FormData
  if (!(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
  }

  return fetch(url, {
    ...options,
    headers
  });
}

function getUserIdFromToken() {
  try {
    const token = getToken();
    if (!token) return null;

    const parts = token.split(".");
    if (parts.length < 2) return null;

    const payloadBase64Url = parts[1];
    const payloadBase64 = payloadBase64Url
      .replace(/-/g, "+")
      .replace(/_/g, "/")
      .padEnd(Math.ceil(payloadBase64Url.length / 4) * 4, "=");

    const payload = JSON.parse(atob(payloadBase64));
    return payload.oid ?? payload.sub ?? null;
  } catch (error) {
    console.warn("Unable to decode user id from token:", error);
    return null;
  }
}

function getToken() {
  return sessionStorage.getItem("id_token");
}

async function isUserLoggedIn() {
  const accounts = msalInstance.getAllAccounts();

  // Nessun account → sicuramente non loggato
  if (accounts.length === 0) {
    return false;
  }

  const account = msalInstance.getActiveAccount();
  msalInstance.setActiveAccount(account);

  try {
    // Prova ad ottenere un token valido (anche silenziosamente)
    await msalInstance.acquireTokenSilent({
      scopes: ["openid", "profile"],
      account: account
    });

    return true;
  } catch (error) {
    // Token non valido / scaduto / serve login
    return false;
  }
}
