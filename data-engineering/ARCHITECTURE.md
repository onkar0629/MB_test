# How Data Engineering Works (Simple Guide)

This guide explains in plain English what `services/data-engineering` does and how it connects to external APIs (like Fathom, ClickUp, and Slack).

---

## 1. The Core Idea in 30 Seconds

* **Twenty CRM** (the rest of this repo) is for **daily actions**: viewing contacts, clicking buttons, and using Hermes AI to create quick tasks.
* **Data Engineering** (`services/data-engineering`) is the **data pipeline**: it automatically fetches copies of all meetings, tasks, and messages from external APIs and stores them permanently in Databricks for reports and analytics.

---

## 2. The Simple Flow

```
[ External APIs ]               [ Our Code in this Repo ]             [ Databricks Cloud ]
(Fathom, ClickUp, Slack)       (services/data-engineering)                 (Storage)

   ┌──────────────┐                 ┌─────────────────┐               ┌─────────────────┐
   │  Fathom API  │  ──1. Fetch──>  │     land.py     │  ──2. Save──> │ Landing Volume  │
   │  (Meetings)  │                 │  (Calls the API)│               │ (Raw JSON files)│
   └──────────────┘                 └─────────────────┘               └────────┬────────┘
                                                                               │
                                    ┌─────────────────┐                        │
                                    │ bronze_task.py  │  <──3. Read────────────┘
                                    │  (Spark Loader) │
                                    └────────┬────────┘
                                             │
                                         4. Append
                                             ▼
                                    ┌─────────────────┐
                                    │  meetings_raw   │
                                    │  (Delta Table)  │
                                    └─────────────────┘
```

---

## 3. Step-by-Step: What Happens on Every Run

1. **Step 1: Check the Watermark**  
   The code looks at a small tracking table to see when it last ran (e.g., *"last checked yesterday at 5 PM"*).

2. **Step 2: Fetch New Data from the API (`land.py`)**  
   It calls the external API (like Fathom) using an API key stored securely in Databricks. It only asks for meetings that happened *after* the last watermark.

3. **Step 3: Save the Raw File (Landing)**  
   It saves the exact response from the API into a folder called **Landing**.  
   *Why?* If we ever need to re-run or fix anything in the future, we still have the original raw data without having to call the API again.

4. **Step 4: Load into a Table (`bronze_task.py`)**  
   A background task reads the new file and inserts it as a new row into the **`meetings_raw`** table.

5. **Step 5: Move Watermark Forward**  
   Once all files are safely stored, the code updates the watermark to *"now"*, so the next run won't download the same meetings twice.

---

## 4. How It Works With Other Third-Party APIs

The exact same pattern applies to any external tool:

| External API | What it fetches | Landing File | Destination Table |
|---|---|---|---|
| **Fathom** | Meeting transcripts, audio links, summaries | `.../meetings/...page-01.json` | `meetings_raw` |
| **ClickUp** | Tasks, spaces, statuses | `.../tasks/...page-01.json` | `tasks_raw` |
| **Slack** | Channel messages, threads | `.../messages/...page-01.json` | `messages_raw` |

---

## 5. Why Is It in This Repo?

Putting `data-engineering` inside `action-os-twenty/services/` keeps everything in **one place**:
* Developers don't need multiple separate repositories.
* Code sharing and CI testing happen together in GitHub Actions.
* All data sources feeding Action OS stay organized and easy to find.
