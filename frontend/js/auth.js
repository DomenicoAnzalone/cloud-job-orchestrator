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

async function apiFetch(url, options = {}) {

  const token = getToken();

  options.headers = {
    ...options.headers,
    "Authorization": `Bearer ${token}`
  };

  return fetch(url, options);
}