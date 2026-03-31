<p align="center">
  <img src="assets/logo.png" width="110"/>
</p>

<p align="center">
  <img src="assets/azure-logo.svg" width="40"/>
</p>

<h1 align="center">Cloud Job Orchestrator</h1>

<p align="center">
  Sistema <b>event-driven</b> per orchestrazione resiliente di job asincroni su Azure
</p>

<p align="center">
  <i>
    Decoupled, scalable and resilient architecture for asynchronous workloads
  </i>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/architecture-event--driven-blue?style=flat" />
  <img src="https://img.shields.io/badge/cloud-Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white" />
  <img src="https://img.shields.io/badge/status-demo--ready-green?style=flat" />
  <img src="https://img.shields.io/badge/license-Apache%202.0-blue?style=flat" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/backend-Azure%20Functions-512BD4?style=flat&logo=azurefunctions&logoColor=white" />
  <img src="https://img.shields.io/badge/frontend-Node.js-339933?style=flat&logo=node.js&logoColor=white" />
  <img src="https://img.shields.io/badge/storage-Blob%20Storage-0078D4?style=flat" />
  <img src="https://img.shields.io/badge/state-Cosmos%20DB-2E8B57?style=flat" />
  <img src="https://img.shields.io/badge/messaging-Service%20Bus-FF6F00?style=flat" />
</p>

---

## Indice

- [Cloud Job Orchestrator](#cloud-job-orchestrator)
- [Architettura ad alto livello](#architettura-ad-alto-livello)
- [Workflow end-to-end](#workflow-end-to-end)
- [Funzionalità principali](#funzionalità-principali)
  - [Job disponibili](#job-disponibili)
- [Servizi Azure usati e perché](#servizi-azure-usati-e-perché)
- [Dettagli di implementazione](#dettagli-di-implementazione)
- [Perché questa architettura](#perché-questa-architettura)
- [Sicurezza e confini](#sicurezza-e-confini)
- [Assunzioni di progetto](#assunzioni-di-progetto)
- [Demo e materiali visivi](#demo-e-materiali-visivi)
- [Troubleshooting](#troubleshooting)
- [Come eseguire il progetto in locale](#come-eseguire-il-progetto-in-locale)
- [Come fare il deploy da zero](#come-fare-il-deploy-da-zero)
- [Limiti attuali e sviluppi futuri](#limiti-attuali-e-sviluppi-futuri)
- [Licenza e crediti](#licenza-e-crediti)

---

## Cloud Job Orchestrator

**Cloud Job Orchestrator** è un sistema **event-driven** per l’orchestrazione di job asincroni, non una semplice app di upload + processing.

Il progetto affronta un problema preciso: gestire in modo resiliente job asincroni separando nettamente:

- **intake** (accettazione richiesta, validazione, persistenza stato);
- **execution** (elaborazione in worker separato);
- **stato centralizzato** su datastore dedicato;
- **decoupling via coda** tra API e worker.

Questo approccio abilita:

- invio job **non bloccante** lato client;
- comportamento **at-least-once** con retry del messaggio;
- scalabilità orizzontale dei worker;
- assorbimento dei picchi tramite buffering su coda;
- tracking stato end-to-end con aggiornamenti realtime e fallback polling.

Il caso d’uso implementato è l’orchestrazione di job immagine asincroni con output persistito su Blob Storage, monitoraggio stato in UI e notifiche realtime per utente/job.
I due job attuali (**background removal** e **image upscale**) sono esempi concreti di una pipeline più generale: il focus principale del progetto è l’architettura che li supporta, non il singolo algoritmo di elaborazione.

---

## Architettura ad alto livello

Componenti principali presenti nel repository:

1. **Frontend Web App (Express)**
   - Server Node/Express che serve l’interfaccia e la configurazione runtime (`/config.js`).
   - UI per autenticazione, creazione job, monitoraggio stato e download output.

2. **Backend API (Azure Functions HTTP)**
   - Endpoint di creazione job (`POST /jobs`), stato (`GET /jobs/{id}`), output link (`GET /jobs/{id}/output-link`) e negoziazione realtime (`/realtime/negotiate`).

3. **Worker (Azure Functions Service Bus trigger)**
   - Consuma messaggi dalla coda `q-jobs`, esegue elaborazione, aggiorna stato, pubblica eventi realtime.

4. **Cosmos DB (source of truth)**
   - Stato job e metadati (status, progress, attempts, input/output reference, error, timestamps).

5. **Azure Service Bus**
   - Coda di disaccoppiamento tra intake e execution, con semantica at-least-once.

6. **Blob Storage**
   - Persistenza dei file input/output e generazione link temporanei di download.

7. **Azure SignalR**
   - Push realtime eventi di stato/progresso/log verso il client associato all’utente.

8. **Microsoft Entra ID**
   - Autenticazione e validazione token JWT per accesso alle API.

9. **Cleanup timer**
   - Funzione schedulata ogni 15 minuti per rimuovere job completati oltre soglia temporale e blob associati.

**Placeholder immagine architetturale:**
> _[Da sostituire con diagramma architetturale ufficiale del sistema.]_

---

## Workflow end-to-end

1. **Creazione job da UI**
   - Utente autenticato seleziona tipo job e file immagine.
2. **Validazione API**
   - Verifica token, tipo job supportato, presenza file.
3. **Upload input**
   - Il file viene caricato nel container Blob input.
4. **Persistenza stato iniziale**
   - Viene creato un documento job in Cosmos DB (stato `queued`).
5. **Enqueue**
   - API pubblica su Service Bus un messaggio leggero (claim-check: identificativi, non payload completo).
6. **Processing worker**
   - Worker legge messaggio, carica input da Blob, applica trasformazione, carica output su Blob.
7. **Aggiornamento stato**
   - Cosmos viene aggiornato progressivamente (`processing` → `done` o `failed`, con progress/attempts/error).
8. **Notifiche realtime**
   - Eventi SignalR inviati al client utente (`status`, `progress`, `log`, `completed`).
9. **Fallback polling**
   - Se realtime non disponibile, frontend continua monitoraggio via polling API.
10. **Download finale**
   - UI richiede `output-link`; backend genera URL SAS temporaneo per scaricare l’output.

---

## Funzionalità principali

Capacità effettivamente implementate:

- autenticazione utente tramite Entra ID;
- creazione job asincroni via API;
- monitoraggio stato job (queued/processing/done/failed);
- gestione output con link di download temporaneo;
- supporto a più `job type`;
- retry/at-least-once behavior del processing basato su coda;
- progress updates durante l’esecuzione;
- cleanup periodico dei job completati;
- fallback automatico realtime → polling lato frontend.

### Job disponibili

1. **background_removal**
   - Usa `rembg` con sessione riutilizzabile lato worker.
   - L’immagine viene convertita e salvata in **PNG** (`image/png`).

2. **image_upscale**
   - Usa Pillow con resize LANCZOS (attualmente fattore 2x).
   - Tenta di preservare il formato originale quando disponibile.

---

## Servizi Azure usati e perché

- **Azure Functions**
  - Ruolo: API HTTP, worker queue-trigger, timer di cleanup.
  - Perché: modello serverless, separazione naturale dei trigger e scaling gestito.

- **Azure Service Bus**
  - Ruolo: coda job `q-jobs`.
  - Perché: disaccoppia intake da execution, bufferizza picchi, abilita retry at-least-once.

- **Azure Cosmos DB**
  - Ruolo: stato e metadati job.
  - Perché: datastore operativo a bassa latenza, usato come **source of truth**.

- **Azure Blob Storage**
  - Ruolo: persistenza input/output e download via SAS temporaneo.
  - Perché: storage adatto a payload binari, separato dai metadati.

- **Azure App Service / Web App**
  - Ruolo: hosting frontend Express.
  - Perché: delivery semplice della UI con configurazione via app settings.

- **Azure SignalR Service**
  - Ruolo: push realtime eventi job a utenti specifici.
  - Perché: aggiornamenti near-real-time senza polling continuo, con fallback quando necessario.

- **Microsoft Entra ID**
  - Ruolo: identity provider per login e protezione API via bearer token.
  - Perché: gestione centralizzata identità/claims in contesto Azure.

- **Application Insights**
  - Ruolo: telemetria e osservabilità runtime.
  - Perché: supporta tuning operativo (es. concorrenza worker, analisi failure/performance).

Best practice applicate nel disegno:

- claim-check pattern nei messaggi;
- decoupling tramite message queue;
- at-least-once processing + idempotenza consumer;
- separazione trigger serverless per responsabilità;
- stato persistito come source of truth;
- realtime push con fallback polling.

Esempio sintetico di documento job in Cosmos DB:

```json
{
  "id": "4f0f3a22-8f2f-4d11-9b3c-4bc29a0d8d77",
  "pk": "a2d7c4d1-3b9e-4d62-8aa1-2f0c0f1f8f44",
  "type": "background_removal",
  "status": "processing",
  "progress": 0.7,
  "attempts": 1,
  "createdAt": "2026-03-31T10:15:20.120000+00:00",
  "updatedAt": "2026-03-31T10:15:42.980000+00:00",
  "correlationId": "f6a6f7cf-6e4f-4d6a-9e81-3d8584df9f18",
  "inputRef": { "container": "input", "blobName": "<pk>/<jobId>/image.jpg" },
  "outputRef": { "container": "output", "blobName": "<pk>/<jobId>/image.png" },
  "error": null,
  "parameters": { ... }
}
```

---

## Dettagli di implementazione

Il progetto è strutturato in moduli e servizi separati, non come demo monolitica:

- backend organizzato in `src/shared/` (utility cross-cutting) e `src/services/` (logica applicativa);
- separazione netta tra API (`create/get status/output-link`) e worker (`process_job_message`);
- claim-check pattern: nel messaggio Service Bus passano solo riferimenti (`jobId`, `pk`, `type`, `correlationId`);
- idempotenza operativa worker: se trova stato terminale (`done`/`canceled`) salta la riesecuzione;
- gestione errori con persistenza stato `failed` e payload errore (`code`, `message`, `stage`);
- progress tracking intermedio (`0.1`, `0.4`, `0.7`, `0.9`, `1.0`) con eventi realtime;
- cleanup automatico dei job `done` più vecchi di 1 ora, con eliminazione blob input/output;
- heartbeat realtime durante operazioni lunghe per evitare “silenzio” lato client.

Scelta operativa rilevante: in `backend/host.json` il trigger Service Bus usa **`maxConcurrentCalls: 2`** per istanza. Questa configurazione è stata mantenuta dopo test/osservazioni su Application Insights per ridurre saturazione risorse e prevenire instabilità/crash delle singole istanze quando i job sono pesanti (es. elaborazioni immagine con librerie native).

---

## Perché questa architettura

Trade-off principale: preferire disaccoppiamento e robustezza operativa rispetto a una pipeline sincrona semplice.

Benefici pratici:

- **disaccoppiamento** UI/API da processing;
- **resilienza** a errori temporanei con retry del messaggio;
- **scalabilità** dei worker in base al carico;
- **isolamento** dei job e del runtime di elaborazione;
- **gestione picchi** tramite coda;
- **riduzione del coupling** tra esperienza utente e tempi reali di processing.

---

## Sicurezza e confini

- **Autenticazione/autorizzazione**
  - Le API applicative richiedono bearer token Entra ID e validano audience/issuer/claims (`oid`, `tid`).

- **Protezione API**
  - Anche se i trigger sono configurati `auth_level=ANONYMOUS`, la protezione applicativa è implementata nel codice con validazione token.

- **Accesso ai Blob**
  - I container input/output sono privati (`publicAccess: None`) e i download sono esposti tramite link SAS con TTL breve.

- **Uso di Entra ID**
  - Frontend acquisisce token via MSAL; backend valida JWT contro JWKS tenant-specifica.

- **Superficie non esposta**
  - Worker Service Bus, accesso diretto a Cosmos e operazioni interne di cleanup non sono endpoint pubblici UI.

---

## Assunzioni di progetto

- La UI è progettata per job relativamente veloci e per il monitoraggio operativo in tempo reale.
- Il sistema è orientato a un utilizzo one-shot: creazione del job, attesa del risultato e download, senza necessità di consultazione successiva.
- Non è previsto uno storico persistente dei job lato frontend dopo refresh completo.
- Il focus è sull’orchestrazione resiliente dei job asincroni, non sull’archiviazione o gestione storica dei risultati.
- La persistenza lato backend (Cosmos DB, Blob Storage) è utilizzata per orchestrazione e tracciamento durante l’esecuzione, non come storage permanente per consultazione utente nel lungo periodo.

---


## Demo e materiali visivi

- **Video demo**: _[placeholder — inserire link video demo]_
- **Diagramma architetturale**: _[placeholder — inserire immagine architettura]_
- **Screenshot UI principali**: _[placeholder — inserire schermate login, create job, monitoraggio, download]_
- **Asset futuri (benchmark, sequence diagram, failure paths)**: _[placeholder — inserire riferimenti]_

---

## Troubleshooting

- **Primo avvio frontend più lento**
  - Il server Express può richiedere tempo iniziale di warm-up.

- **Primo avvio Functions più lento**
  - Cold start + caricamento runtime/librerie (in particolare `rembg`) può aumentare latenza iniziale.

- **Latenza demo intenzionale**
  - Il worker può introdurre ritardo configurabile (`WORKER_DELAY_SECONDS`) per visualizzare meglio le transizioni in demo.

- **Login/token issues**
  - Verificare coerenza configurazioni Entra ID: tenant, client ID, audience, scope e app registration.

- **Deploy/config issues**
  - Usare `infra/README.md` e script `infra/` per diagnosi (parametri, app settings, prerequisiti).

- **Concorrenza worker e stabilità**
  - In caso di saturazione o errori runtime sotto carico, verificare impostazione `maxConcurrentCalls: 2` in `backend/host.json` (scelta introdotta dopo osservazioni su Application Insights).

---

## Come eseguire il progetto in locale

### 1) Prerequisiti

- Azure Functions Core Tools
- Python 3 + pip
- Node.js + npm
- Azure CLI (`az`)
- Per eseguire il flusso completo con infrastruttura Azure: seguire le istruzioni in [infra/README.md](infra/README.md)

### 2) Configurazione variabili

1. Backend:
   ```bash
   cp backend/local.settings.example.json backend/local.settings.json
   ```
2. Frontend:
   ```bash
   cp .env.example .env
   ```
3. Popolare valori reali (API base URL, Entra ID, connessioni backend).

Suggerimento pratico: dopo deploy infrastruttura usare `infra/setup-env.sh` per estrarre valori da App Settings.

### 3) Avvio backend

Da `backend/`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
func start
```

### 4) Avvio frontend

Da `frontend/`:

```bash
npm install
npm start
```

Il frontend Express espone la UI (porta locale da `PORT`, default `3000`).

### 5) Test rapido demo

1. Login Microsoft dalla UI.
2. Seleziona `background_removal` o `image_upscale`.
3. Carica immagine e crea job.
4. Verifica transizioni stato/progresso (realtime o polling fallback).
5. Scarica output con pulsante dedicato quando lo stato è `done`.

---

## Come fare il deploy da zero

Il flusso ufficiale di provisioning e deploy è nella cartella [infra/](infra/)

- Provisioning risorse: `infra/deploy.sh` + `infra/bicep/main.bicep`
- Parametri ambiente: `infra/bicep/parameters*.json`
- Estrazione variabili runtime: `infra/setup-env.sh`
- Istruzioni operative complete: **`infra/README.md`**

Questo README non duplica la guida infrastrutturale: per deploy da zero seguire direttamente [infra/README.md](infra/README.md).

---

## Limiti attuali e sviluppi futuri

Evoluzioni naturali del progetto:

- supporto a più tipi di input e validazioni avanzate;
- pipeline di elaborazione più realistica (multi-step, chaining, policy);
- multi-tenant completo con isolamento/logica quota;
- osservabilità avanzata (metriche business + tracing distribuito approfondito);
- persistenza storica job lato frontend o datastore dedicato per consultazione lunga;
- evoluzione da flusso orientato a job veloci verso piattaforma operativa più completa.

---

## Licenza e crediti

- **Licenza**: fare riferimento al file `LICENSE` presente nella root del repository.
- **Crediti tecnici**:
  - Azure Functions, Service Bus, Cosmos DB, Blob Storage, SignalR, App Service;
  - librerie Python usate nel processing: `rembg`, `Pillow`.
