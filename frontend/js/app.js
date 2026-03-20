const els = {
    apiBase: document.getElementById("apiBase"),
    pk: document.getElementById("pk"),
    forceFail: document.getElementById("forceFail"),
    createBtn: document.getElementById("createBtn"),
    refreshBtn: document.getElementById("refreshBtn"),
    downloadBtn: document.getElementById("downloadBtn"),
    jobIdValue: document.getElementById("jobIdValue"),
    statusValue: document.getElementById("statusValue"),
    progressValue: document.getElementById("progressValue"),
    attemptsValue: document.getElementById("attemptsValue"),
    errorBox: document.getElementById("errorBox"),
    downloadInfo: document.getElementById("downloadInfo"),
    logBox: document.getElementById("logBox"),
    realtimeValue: document.getElementById("realtimeValue"),
    loginBtn: document.getElementById("loginBtn"),
    logoutBtn: document.getElementById("logoutBtn"),
    state: document.getElementById("loginStatus"),
};

let currentJobId = null;
let pollHandle = null;
let signalrConnection = null;
let realtimeConnected = false;

window.onload = async () => {
    consoleLog("Application loading...");

    const isLogged = await isUserLoggedIn();

    if (!isLogged) {
        document.getElementById("app").classList.add("blurred");
        document.getElementById("authOverlay").style.display = "flex";
        els.state.textContent = "Not authenticated. Please log in.";
    } else {
        consoleLog("Existing authentication token found...");
        document.getElementById("authOverlay").style.display = "none";
        document.getElementById("subtitle").textContent = `Bentornado: ${msalInstance.getActiveAccount().name}`;
    }
};

async function authentication() {
    els.loginBtn.disabled = true;
    els.state.textContent = "Opening Microsoft login...";

    let loginSucceeded = false;

    try {
        await login();
        loginSucceeded = true;
    } catch (e) {
        els.state.textContent = "Login cancelled or failed.";
    }

    const ok = await isUserLoggedIn();

    if (ok) {
        document.getElementById("authOverlay").style.display = "none";
        document.getElementById("app").classList.remove("blurred");
        document.getElementById("subtitle").textContent = `Bentornato: ${msalInstance.getActiveAccount().name}`;
        els.state.textContent = "";
    } else if (loginSucceeded) {
        // login ok ma stato non valido → raro ma gestito
        els.state.textContent = "Login completed but session not ready.";
    }

    els.loginBtn.disabled = false;
}

async function loggingout() {
    logout();

    document.getElementById("authOverlay").style.display = "flex";
    document.getElementById("app").classList.add("blurred");
    document.getElementById("subtitle").textContent = `Bentornato:`;
    els.state.textContent = "";
}

function setRealtimeStatus(value) {
    els.realtimeValue.textContent = value;
}

function applyJobSnapshot(data, source = "realtime") {
    const status = (data.status || "unknown").toLowerCase();

    setStatus(status);
    els.progressValue.textContent = data.progress ?? "-";
    els.attemptsValue.textContent = data.attempts ?? "-";
    els.errorBox.textContent = formatError(data.error);

    if (status === "done") {
    els.downloadBtn.disabled = false;
    log(`Job reached terminal state via ${source}: done`);
    } else if (status === "failed" || status === "canceled") {
    els.downloadBtn.disabled = true;
    log(`Job reached terminal state via ${source}: ${status}`);
    } else {
    els.downloadBtn.disabled = true;
    }
}

async function disconnectRealtime() {
    if (signalrConnection) {
    try {
        await signalrConnection.stop();
    } catch (_) {
        // ignore
    }
    }

    signalrConnection = null;
    realtimeConnected = false;
    setRealtimeStatus("disconnected");
}

async function connectRealtime() {

    await disconnectRealtime();

    const negotiateUrl =
    `${getApiBase()}/realtime/negotiate?pk=${encodeURIComponent(getPk())}`;

    setRealtimeStatus("connecting");

    try {

    const negotiateRes = await apiFetch(negotiateUrl);
    const negotiateData = await negotiateRes.json();

    signalrConnection = new signalR.HubConnectionBuilder()
        .withUrl(negotiateData.url, {
        accessTokenFactory: () => negotiateData.accessToken
        })
        .withAutomaticReconnect()
        .configureLogging(signalR.LogLevel.Warning)
        .build();

    signalrConnection.on("jobUpdated", (payload) => {

        if (!payload || !payload.jobId) return;

        if (!currentJobId || payload.jobId !== currentJobId) return;

        applyJobSnapshot(payload, "realtime");

        log(
        `Realtime update: status=${payload.status ?? "-"} progress=${payload.progress ?? "-"} attempts=${payload.attempts ?? "-"}`
        );

    });

    signalrConnection.onreconnecting(() => {
        realtimeConnected = false;
        setRealtimeStatus("reconnecting");
        startPolling();
        log("Realtime reconnecting...");
    });

    signalrConnection.onreconnected(() => {
        realtimeConnected = true;
        setRealtimeStatus("connected");
        stopPolling()
        log("Realtime reconnected.");
    });

    signalrConnection.onclose(() => {
        realtimeConnected = false;
        setRealtimeStatus("disconnected");

        startPolling();

        log("Realtime disconnected, polling activated.");
    });

    await signalrConnection.start();

    realtimeConnected = true;
    setRealtimeStatus("connected");

    stopPolling();

    log(`Realtime connected for pk=${getPk()}`);

    } catch (err) {

    setRealtimeStatus("error");
    log(`Realtime connection failed: ${err.message}`);

    }
}

function consoleLog(message) {
    console.log(message);
    log(`Log: ${message}`);
}

function log(message) {
    const ts = new Date().toLocaleTimeString();
    els.logBox.textContent = `[${ts}] ${message}\n` + els.logBox.textContent;
}

function setStatus(status) {
    const normalized = (status || "unknown").toLowerCase();
    els.statusValue.textContent = normalized;
    els.statusValue.className = `status-pill status-${normalized}`;
}

function resetView() {
    currentJobId = null;
    stopPolling();

    els.jobIdValue.textContent = "-";
    els.progressValue.textContent = "-";
    els.attemptsValue.textContent = "-";
    els.errorBox.textContent = "No error";
    els.downloadInfo.textContent = "No output link requested yet";
    setStatus("idle");
    setRealtimeStatus("disconnected");

    els.refreshBtn.disabled = true;
    els.downloadBtn.disabled = true;
}

function stopPolling() {
    if (pollHandle) {
    clearInterval(pollHandle);
    pollHandle = null;
    }
}

function startPolling() {
    stopPolling();

    pollHandle = setInterval(async () => {
    try {
        await refreshStatus();
    } catch (_) {
        // already logged
    }
    }, realtimeConnected ? 15000 : 3000);
}

function getApiBase() {
    return els.apiBase.value.trim().replace(/\/+$/, "");
}

function getPk() {
    return els.pk.value.trim() || "demo-user";
}

async function parseResponse(res) {
    const text = await res.text();
    let data = {};

    try {
    data = text ? JSON.parse(text) : {};
    } catch {
    data = { raw: text };
    }

    if (!res.ok) {
    const msg = data.error || data.message || data.raw || `${res.status} ${res.statusText}`;
    throw new Error(msg);
    }

    return data;
}

function formatError(errorPayload) {
    if (!errorPayload) {
    return "No error";
    }

    if (typeof errorPayload === "string") {
    return errorPayload;
    }

    return JSON.stringify(errorPayload, null, 2);
}

async function createJob() {
    resetView();

    const payload = {
        pk: getPk(),
        type: "csv_cleaning_validation",
        parameters: {
            delimiter: ",",
            trimWhitespace: true,
        },
    };

    if (els.forceFail.checked) {
    payload.parameters.fail = true;
    }

    log("Creating job...");

    els.createBtn.disabled = true;

    try {
    const res = await apiFetch(`${getApiBase()}/jobs`, {
        method: "POST",
        headers: {
        "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });

    const data = await parseResponse(res);

    currentJobId = data.jobId;
    els.jobIdValue.textContent = currentJobId;
    els.refreshBtn.disabled = false;

    log(`Job created successfully. jobId=${currentJobId}`);

    await connectRealtime();
    await refreshStatus();

    if(!realtimeConnected){
        startPolling();
    }

    } catch (err) {
    log(`Create job failed: ${err.message}`);
    setStatus("failed");
    els.errorBox.textContent = err.message;
    } finally {
    els.createBtn.disabled = false;
    }
}

async function refreshStatus() {

    if (!currentJobId) {
        log("No job available to refresh.");
        return;
    }

    const url = `${getApiBase()}/jobs/${encodeURIComponent(currentJobId)}?pk=${encodeURIComponent(getPk())}`;
    const res = await apiFetch(url, { method: "GET" });
    const data = await parseResponse(res);

    applyJobSnapshot(data, "poll");

    log(
        `Status refreshed: status=${data.status ?? "-"} progress=${data.progress ?? "-"} attempts=${data.attempts ?? "-"}`
    );

    const status = (data.status || "unknown").toLowerCase();

    if (status === "done" || status === "failed" || status === "canceled") {
        
    }
}

async function downloadOutput() {
    if (!currentJobId) {
    log("No job available for output download.");
    return;
    }

    log("Requesting output link...");

    try {
    const url = `${getApiBase()}/jobs/${encodeURIComponent(currentJobId)}/output-link?pk=${encodeURIComponent(getPk())}`;
    const res = await apiFetch(url, { method: "GET" });
    const data = await parseResponse(res);

    els.downloadInfo.textContent = JSON.stringify(data, null, 2);
    log("Output link received. Opening browser tab...");

    window.open(data.downloadUrl, "_blank", "noopener,noreferrer");
    } catch (err) {
    els.downloadInfo.textContent = err.message;
    log(`Output link request failed: ${err.message}`);
    }
}

els.createBtn.addEventListener("click", createJob);
els.refreshBtn.addEventListener("click", refreshStatus);
els.downloadBtn.addEventListener("click", downloadOutput);
if (els.loginBtn) {
    els.loginBtn.addEventListener("click", authentication);
}

if (els.logoutBtn) {
    els.logoutBtn.addEventListener("click", logout);
}

resetView();