const els = {
    jobType: document.getElementById("jobType"),
    imageFile: document.getElementById("imageFile"),
    createBtn: document.getElementById("createBtn"),
    refreshBtn: document.getElementById("refreshBtn"),
    loginBtn: document.getElementById("loginBtn"),
    logoutBtn: document.getElementById("logoutBtn"),
    state: document.getElementById("loginStatus"),
    jobsContainer: document.getElementById("jobsContainer"),
    jobCardTemplate: document.getElementById("jobCardTemplate"),
};

let currentUserId = null;
let pollHandle = null;
let signalrConnection = null;
let apiBaseUrl = "http://localhost:7071/api";

let pollingInProgress = false;
let realtimeConnected = false;

const jobsMap = new Map(); // jobId -> { data + domRefs }
const allowedJobTypes = ["background_removal", "image_upscale"];

window.onload = async () => {
    log("Application loading...");
    await loadApiBaseFromSettings();

    const isLogged = await isUserLoggedIn();

    if (!isLogged) {
        document.getElementById("app").classList.add("blurred");
        document.getElementById("authOverlay").style.display = "flex";
        els.state.textContent = "Not authenticated. Please log in.";
    } else {
        log("Existing authentication token found...");
        document.getElementById("authOverlay").style.display = "none";

        const account =
        msalInstance.getActiveAccount() ?? msalInstance.getAllAccounts()[0] ?? null;

        if (account) {
            msalInstance.setActiveAccount(account);
        }

        currentUserId = getUserIdFromToken();

        const displayName = account?.name ?? account?.username ?? "utente";
        document.getElementById("subtitle").textContent = `Bentornato: ${displayName}`;
    }
};

async function authentication() {
    els.loginBtn.disabled = true;
    els.state.textContent = "Opening Microsoft login...";

    let loginSucceeded = false;

    try {
        await login();
        currentUserId = getUserIdFromToken();
        loginSucceeded = true;
    } catch (e) {
        console.error("Login failed:", e);
        els.state.textContent = "Login cancelled or failed.";
    }

    const ok = await isUserLoggedIn();

    if (ok) {
        document.getElementById("authOverlay").style.display = "none";
        document.getElementById("app").classList.remove("blurred");
        const account = msalInstance.getActiveAccount() ?? msalInstance.getAllAccounts()[0] ?? null;

        const displayName = account?.name ?? account?.username ?? "utente";
        document.getElementById("subtitle").textContent = `Bentornato: ${displayName}`;
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
    `${getApiBase()}/realtime/negotiate?pk=${encodeURIComponent(currentUserId)}`;

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

    log(`Realtime connected for pk=${currentUserId}`);

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

    if (event.jobType && job.jobTypeEl) {
        job.jobType = event.jobType;
        job.jobTypeEl.textContent = event.jobType;
    }
    if (event.filename && job.filenameEl) {
        job.filename = event.filename;
        job.filenameEl.textContent = event.filename;
    }

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
                    const url = `${getApiBase()}/jobs/${encodeURIComponent(jobId)}/output-link?pk=${encodeURIComponent(currentUserId)}`;
                    const res = await apiFetch(url);
                    const data = await parseResponse(res);

                    console.log("DOWNLOAD RESPONSE:", data);

                    if (!data.downloadUrl) {
                        throw new Error("Missing download URL");
                    }

                    const link = document.createElement("a");
                    link.href = data.downloadUrl;
                    link.download = job.filename || `${jobId}.png`;
                    link.rel = "noopener noreferrer";
                    link.style.display = "none";
                    document.body.appendChild(link);
                    link.click();
                    link.remove();

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
    const filenameEl = clone.querySelector(".filename");
    const jobTypeEl = clone.querySelector(".job-type");
    const logBox = clone.querySelector(".logBox");
    const downloadBtn = clone.querySelector(".downloadBtn");

    jobIdEl.textContent = jobId;

    jobsContainer.prepend(card);

    return {
        card,
        statusEl,
        progressEl,
        filenameEl,
        jobTypeEl,
        logBox,
        downloadBtn
    };
}

function log(message) {
    const ts = new Date().toLocaleTimeString();
    console.log(`[${ts}] ${message}`);
}

function resetView() {
    stopPolling();

    setRealtimeStatus("disconnected");

    els.refreshBtn.disabled = true;
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
    return apiBaseUrl.trim().replace(/\/+$/, "");
}

async function loadApiBaseFromSettings() {
    const candidates = [
        "/local.settings.json",
        "/backend/local.settings.json",
        "../backend/local.settings.json",
    ];

    for (const path of candidates) {
        try {
            const res = await fetch(path, { cache: "no-store" });
            if (!res.ok) continue;
            const data = await res.json();
            const value =
                data?.Values?.FRONTEND_API_BASE_URL ||
                data?.Values?.API_BASE_URL ||
                data?.Values?.CJO_API_BASE_URL;
            if (value) {
                apiBaseUrl = value;
                log(`API base loaded from ${path}`);
                return;
            }
        } catch (_) {
            // fallback
        }
    }

    log("Using default API base URL.");
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

    const image = els.imageFile.files?.[0];
    if (!image) {
        log("Create job failed: please select an image file.");
        return;
    }

    const selectedJobType = getSelectedJobType();
    if (!selectedJobType) {
        log(`Create job failed: unsupported job type "${els.jobType.value}".`);
        return;
    }

    const formData = new FormData();
    formData.append("type", selectedJobType);
    formData.append("image", image);

    log(`Creating job with type=${selectedJobType}...`);

    els.createBtn.disabled = true;

    if (!signalrConnection) {
        await connectRealtime();
    }

    try {
    const res = await apiFetch(`${getApiBase()}/jobs`, {
        method: "POST",
        body: formData,
    });

    const data = await parseResponse(res);

    const jobId = data.jobId;

    // forza creazione card immediata
    handleJobEvent({
        jobId,
        type: "status",
        status: "creating",
        jobType: selectedJobType,
        filename: image.name
    });
    els.refreshBtn.disabled = false;

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

function getSelectedJobType() {
    const selectedType = (els.jobType?.value || "").trim();
    return allowedJobTypes.includes(selectedType) ? selectedType : null;
}

async function refreshJobStatus(jobId) {
    const url = `${getApiBase()}/jobs/${encodeURIComponent(jobId)}?pk=${encodeURIComponent(currentUserId)}`;
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
                const job = jobsMap.get(jobId);
                if (!job) {
                    return;
                }

                if (job.status === "done" || job.status === "failed") {
                    return;
                }

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

els.createBtn.addEventListener("click", createJob);
els.refreshBtn.addEventListener("click", refreshAllJobs);
if (els.loginBtn) {
    els.loginBtn.addEventListener("click", authentication);
}

if (els.logoutBtn) {
    els.logoutBtn.addEventListener("click", logout);
}

resetView();
