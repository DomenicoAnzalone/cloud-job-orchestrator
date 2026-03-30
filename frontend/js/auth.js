const redirectUri =
  location.hostname === "localhost"
    ? "http://localhost:5500"
    : window.location.origin;

const msalConfig = {
  auth: {
    clientId: "325c2c85-0dd0-4593-b448-eecd4d268881",
    authority: "https://login.microsoftonline.com/1ff1ab6f-5116-43af-a48b-d8da1301df40",
    redirectUri: redirectUri
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
    scopes: ["api://52836a8b-6649-49ac-acbe-53caeccd542f/access_as_user"],
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
