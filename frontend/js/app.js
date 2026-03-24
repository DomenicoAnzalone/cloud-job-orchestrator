const els = {
    apiBase: document.getElementById("apiBase"),
    forceFail: document.getElementById("forceFail"),
    createBtn: document.getElementById("createBtn"),
    refreshBtn: document.getElementById("refreshBtn"),
    downloadBtn: document.getElementById("downloadBtn"),
    loginBtn: document.getElementById("loginBtn"),
    logoutBtn: document.getElementById("logoutBtn"),
    state: document.getElementById("loginStatus"),
    jobsContainer: document.getElementById("jobsContainer"),
    jobCardTemplate: document.getElementById("jobCardTemplate"),
};

let currentUserId = null;
let currentJobId = null;
let pollHandle = null;
let signalrConnection = null;

let pollingInProgress = false;
let realtimeConnected = false;

const jobsMap = new Map(); // jobId -> { data + domRefs }

window.onload = async () => {
    log("Application loading...");

    const isLogged = await isUserLoggedIn();

    if (!isLogged) {
        document.getElementById("app").classList.add("blurred");
        document.getElementById("authOverlay").style.display = "flex";
        els.state.textContent = "Not authenticated. Please log in.";
    } else {
        log("Existing authentication token found...");
        document.getElementById("authOverlay").style.display = "none";
        const account = msalInstance.getActiveAccount();
        currentUserId = getUserIdFromToken();

        document.getElementById("subtitle").textContent =
        `Bentornato: ${account.name}`;

        document.getElementById("pkDisplay").textContent = currentUserId;
    }
};

async function authentication() {
    els.loginBtn.disabled = true;
    els.state.textContent = "Opening Microsoft login...";

    let loginSucceeded = false;

    try {
        await login();
        currentUserId = getUserIdFromToken();
        document.getElementById("pkDisplay").textContent = currentUserId;
        loginSucceeded = true;
    } catch (e) {
        console.error("Login failed:", e);
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

function setRealtimeStatus(value) {
    // temporaneamente disabilitato (no elemento in UI)
    console.log("Realtime status:", value);
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

    signalrConnection.on("jobUpdated", (event) => {

        if (!event || !event.jobId) return;

        handleJobEvent(event);

    });

    signalrConnection.onreconnecting(() => {
        realtimeConnected = false;
        setRealtimeStatus("reconnecting");
        startPolling();
        refreshAllJobs().catch(() => {});
        log("Realtime reconnecting...");
    });

    signalrConnection.onreconnected(() => {
        realtimeConnected = true;
        setRealtimeStatus("connected");
        stopPolling();
        refreshAllJobs().catch(() => {});
        log("Realtime reconnected.");
    });

    signalrConnection.onclose(() => {
        realtimeConnected = false;
        setRealtimeStatus("disconnected");
        startPolling();
        refreshAllJobs().catch(() => {});
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

function handleJobEvent(event) {

    if (!event.type) {
        console.warn("Malformed event", event);
        return;
    }

    const jobId = event.jobId;

    if (!jobsMap.has(jobId)) {

        const dom = createJobCard(jobId);

        jobsMap.set(jobId, {
            jobId,
            status: "unknown",
            progress: 0,
            logs: [],
            ...dom
        });
    }

    const job = jobsMap.get(jobId);

    switch (event.type) {

        case "status":
            job.status = event.status;
            job.statusEl.textContent = event.status;
            break;

        case "progress":
            job.status = event.status ?? job.status;
            job.progress = event.progress ?? job.progress;

            job.statusEl.textContent = job.status;
            job.progressEl.textContent = job.progress;
            break;

        case "log":
            job.logs.push(event.message);
            job.logBox.textContent =
                `[${new Date().toLocaleTimeString()}] ${event.message}\n` +
                job.logBox.textContent;
            break;

        case "completed":
            job.status = "done";
            job.progress = 1;
            job.statusEl.textContent = "done";
            job.progressEl.textContent = job.progress;

            job.downloadBtn.disabled = false;

            job.downloadBtn.onclick = async () => {
                try {
                    const url = `${getApiBase()}/jobs/${encodeURIComponent(jobId)}/output-link?pk=${encodeURIComponent(getPk())}`;
                    const res = await apiFetch(url);
                    const data = await parseResponse(res);

                    console.log("DOWNLOAD RESPONSE:", data);

                    if (!data.downloadUrl) {
                        throw new Error("Missing download URL");
                    }

                    window.open(data.downloadUrl, "_blank", "noopener,noreferrer");

                } catch (err) {
                    log(`Download failed: ${err.message}`);
                }
            };

            break;

        case "failed":
            job.status = "failed";
            job.statusEl.textContent = "failed";

            job.error = event.error;

            job.logBox.textContent =
                `[ERROR] ${JSON.stringify(event.error)}\n` +
                job.logBox.textContent;
            break;
    }

    log(`Job ${jobId} updated: type=${event.type}`);
}

function createJobCard(jobId) {

    const clone = jobCardTemplate.content.cloneNode(true);

    const card = clone.querySelector(".job-card");
    const jobIdEl = clone.querySelector(".job-id");
    const statusEl = clone.querySelector(".status");
    const progressEl = clone.querySelector(".progress");
    const logBox = clone.querySelector(".logBox");
    const downloadBtn = clone.querySelector(".downloadBtn");

    jobIdEl.textContent = jobId;

    jobsContainer.prepend(card);

    return {
        card,
        statusEl,
        progressEl,
        logBox,
        downloadBtn
    };
}

function log(message) {
    const ts = new Date().toLocaleTimeString();
    console.log(`[${ts}] ${message}`);
}

function resetView() {
    currentJobId = null;
    stopPolling();

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
            await refreshAllJobs();
        } catch (_) {
            // already logged
        }
    }, realtimeConnected ? 15000 : 3000);
}

function getApiBase() {
    return els.apiBase.value.trim().replace(/\/+$/, "");
}

function getPk() {
    return currentUserId;
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

    if (!signalrConnection) {
        await connectRealtime();
    }

    try {
    const res = await apiFetch(`${getApiBase()}/jobs`, {
        method: "POST",
        headers: {
        "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });

    const data = await parseResponse(res);

    const jobId = data.jobId;
    currentJobId = jobId;

    // forza creazione card immediata
    handleJobEvent({
        jobId,
        type: "status",
        status: "creating"
    });
    els.refreshBtn.disabled = false;

    log(`Job created successfully. jobId=${currentJobId}`);

    await refreshJobStatus(jobId);

    if(!realtimeConnected){
        startPolling();
    }

    } catch (err) {
        log(`Create job failed: ${err.message}`);
    } finally {
        els.createBtn.disabled = false;
    }
}

async function refreshJobStatus(jobId) {
    const url = `${getApiBase()}/jobs/${encodeURIComponent(jobId)}?pk=${encodeURIComponent(getPk())}`;
    const res = await apiFetch(url, { method: "GET" });
    const data = await parseResponse(res);

    handleJobEvent({
        jobId,
        type: "progress",
        status: data.status,
        progress: data.progress
    });

    const status = (data.status || "unknown").toLowerCase();

    if (status === "done") {
        handleJobEvent({
            jobId,
            type: "completed"
        });
    } else if (status === "failed") {
        handleJobEvent({
            jobId,
            type: "failed",
            error: data.error
        });
    }

    return data;
}

async function refreshAllJobs() {
    if (pollingInProgress) {
        return;
    }

    pollingInProgress = true;

    try {
        const jobIds = Array.from(jobsMap.keys());

        if (jobIds.length === 0) {
            log("No jobs available to refresh.");
            return;
        }

        const results = await Promise.allSettled(
            jobIds.map(async (jobId) => {
                const data = await refreshJobStatus(jobId);
                log(
                    `Status refreshed: job=${jobId} status=${data.status ?? "-"} progress=${data.progress ?? "-"} attempts=${data.attempts ?? "-"}`
                );
            })
        );

        const failedCount = results.filter((r) => r.status === "rejected").length;
        if (failedCount > 0) {
            log(`Polling completed with ${failedCount} failed refresh(es).`);
        }
    } finally {
        pollingInProgress = false;
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

    log(`Download URL received: ${data.url}`);
    log(`Download URL received: ${data.downloadUrl}`);

    if (!data.downloadUrl) {
        throw new Error("Missing download URL in response");
    }

    window.open(data.downloadUrl, "_blank", "noopener,noreferrer");
    } catch (err) {
        log(`Output link request failed: ${err.message}`);
    }
}

els.createBtn.addEventListener("click", createJob);
els.refreshBtn.addEventListener("click", refreshAllJobs);
els.downloadBtn.addEventListener("click", downloadOutput);
if (els.loginBtn) {
    els.loginBtn.addEventListener("click", authentication);
}

if (els.logoutBtn) {
    els.logoutBtn.addEventListener("click", logout);
}

resetView();