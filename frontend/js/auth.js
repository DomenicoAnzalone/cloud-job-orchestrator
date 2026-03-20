const msalConfig = {
  auth: {
    clientId: "325c2c85-0dd0-4593-b448-eecd4d268881",
    authority: "https://login.microsoftonline.com/1ff1ab6f-5116-43af-a48b-d8da1301df40",
    redirectUri: "http://localhost:5500" 
  } 
};

const msalInstance = new msal.PublicClientApplication(msalConfig);

async function login() {
  const loginResponse = await msalInstance.loginPopup({
    scopes: ["openid", "profile"]
  });

  sessionStorage.setItem("id_token", loginResponse.idToken);
  consoleLog("Login successful. Authentication token stored.");
  msalInstance.setActiveAccount(loginResponse.account);
}

async function logout() {
  // pulizia stato locale
  msalInstance.setActiveAccount(null);
  sessionStorage.removeItem("id_token");

  consoleLog("User logged out.");

  // reload app
  window.location.reload();
}

async function getAccessToken() {
  const account = msalInstance.getActiveAccount();

  if (!account) {
    throw new Error("No active account");
    // TO-DO : forzare il login se non c'è un account attivo, invece di lanciare un errore
  }

  const response = await msalInstance.acquireTokenSilent({
    scopes: ["api://52836a8b-6649-49ac-acbe-53caeccd542f/access_as_user"],
    account: account
  });

  return response.accessToken;
}

// Wrapper per fetch che include automaticamente il token di accesso
async function apiFetch(url, options = {}) {
  const accessToken = await getAccessToken();

  return fetch(url, {
    ...options,
    headers: {
      ...options.headers,
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json"
    }
  });
}

function getUserIdFromToken() {
  const token = getToken();

  if (!token) return null;

  const payload = JSON.parse(atob(token.split(".")[1]));
  return payload.oid;
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
