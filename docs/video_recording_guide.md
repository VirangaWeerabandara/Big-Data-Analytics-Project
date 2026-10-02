# Video recording guide (8–9 minute demo)

A step-by-step guide for recording the project demo solo, plus a full voiceover script you can
read live while recording or record separately and sync in editing. It follows the same beats
as `docs/demo_script.md` (written for a live/viva walkthrough) but is restructured for a single
person recording alone: what to set up beforehand, what to click/type in order, and exactly
what to say.

Two ways to use this file:
- **Live narration** — talk while you record, following the [DO] / [SAY] pairs in real time.
- **Record then dub** — record the screen actions silently following [DO] only, then read the
  **Full voiceover script** at the end in one take against the recording in your editor.

---

## 1. Equipment and software

- **Screen recorder:** QuickTime Player (Mac, built in, free) — File → New Screen Recording.
  Records system audio + mic in one file, good enough for this.
- **Microphone:** even AirPods/laptop mic is fine; record in a quiet room, avoid the room's
  built-in fan noise if you can (close the laptop lid vents area, sit away from the machine).
- **Editor (optional, for trimming/cutting only):** iMovie is enough — you only need to cut
  dead air and splice one sped-up clip (§5 below). No transitions or effects needed.

## 2. One-time setup (before you press record)

```bash
cd "/Users/viranga/Desktop/Semester 8/Blockchain /Project"
make up
```

Wait **at least 10–15 minutes** with the stack running before recording, so there is real
deteriorating-patient data, at least one alert, and at least one generated report. Recording
against a freshly started, empty system looks unconvincing.

While waiting, prepare:

1. **Increase font sizes.**
   - Terminal: Settings → Profiles → Text → bump to 18–20 pt.
   - Browser: zoom to 125–150 % (⌘+ a few times) on every tab you'll show.
2. **Find a sepsis patient id** for later:
   ```bash
   make psql
   SELECT patient_id FROM patients WHERE scenario='sepsis';
   \q
   ```
   Write the id down (e.g. `P014`) — you'll use it in Scene 3.
3. **Open these browser tabs in order** (so ⌘1–⌘6 jump straight to them):
   1. Mermaid diagram from `docs/architecture_decision.md` §4 (open the file in a Markdown
      preview, or paste the mermaid block into https://mermaid.live for a clean full-screen view)
   2. Grafana → Ward Monitoring → *Ward vitals pipeline* dashboard (`localhost:3000`)
   3. The latest `reports/risk_report_*.html` file, opened directly in the browser
      (`open reports/risk_report_$(ls reports/*.html | tail -1 | sed 's/.*risk_report_//;s/.html//').html`)
   4. Airflow UI (`localhost:8080`), graph view of `lab_ingest`
   5. Airflow UI, graph view of `daily_risk_report`
   6. Prometheus alerts page (`localhost:9090/alerts`)
4. **Open one terminal window**, `cd` into the project root, font already bumped, history
   cleared (`clear`) so nothing old is visible.
5. **Close everything else** — Slack, notifications, other browser windows. Turn on
   Do Not Disturb (Mac: Control Centre → Focus).
6. **Run a 10-second test recording** first and check audio levels aren't clipping.

## 3. Recording — step by step

Recommended: record in **two takes** rather than one continuous 8-minute take, because Scene 6
requires waiting ~75 real seconds for an alert to fire. Splitting means one slip doesn't ruin
the whole video.

- **Take A** = Scenes 1–5 (everything except the live alert wait)
- **Take B** = Scene 6 only (the alert-firing clip, sped up 3–4× in the editor)
- Record the closing (Scene 7) at the end of Take A, then cut Take B in before it.

Follow each [DO] step exactly, then say the matching [SAY] line at a natural pace — don't rush,
pause briefly between sentences, it's easier to trim silence than to fix a rushed line.

---

### Scene 1 — The problem and the decision (≈0:00–1:00)

**[DO]** Switch to tab 1, the architecture diagram. Let it sit on screen, don't scroll yet.

**[SAY]**
> "This is a near-real-time monitoring pipeline for hospital ward vital signs. A ward needs to
> know which patients are deteriorating right now, and how yesterday's lab results change that
> picture. Vitals stream in from bedside monitors every second; lab results arrive once a day
> from pathology."

**[DO]** Point at / hover over the diagram's Kafka box, then the Spark box.

**[SAY]**
> "We chose a Kappa architecture: both vitals and lab results flow through Kafka, and a single
> Spark Structured Streaming job computes everything downstream. The deciding reason is
> consistency — there's exactly one scoring implementation, shared by the live API and the
> daily report, so they can never disagree about the same patient. We rejected a Lambda
> architecture because its separate batch layer would duplicate that scoring logic just to
> process around a hundred lab rows a day — infrastructure without a workload."

---

### Scene 2 — Data flowing in (≈1:00–2:00)

**[DO]** Switch to the terminal.

```bash
make consume n=3
```

**[SAY]**
> "Here's live traffic on the vitals topic — JSON readings keyed by patient id, so each
> patient's readings stay in order for trend detection."

**[DO]** Run (note: plain `make logs` follows forever and never reaches EOF, so piping it
into `tail` shows nothing — query a bounded window instead):

```bash
docker compose logs --no-log-prefix --tail=2000 vitals-simulator | grep deterioration | tail -3
```

**[SAY]**
> "Every simulated patient follows a hidden clinical storyline — sepsis, respiratory failure,
> haemorrhage, or stable — with gradual deterioration episodes, sudden spikes, and
> deliberately broken, late, and duplicate messages mixed in."

**[DO]** Run:

```bash
make clock-show
```

**[SAY]**
> "And everything runs on a simulated clock — one simulated day passes every five real
> minutes — used consistently across every component."

---

### Scene 3 — Real-time monitoring (≈2:00–3:30)

**[DO]** Switch to tab 2, the Grafana dashboard. Let the KPI row sit for 2–3 seconds, then
scroll slowly down through the Processing section.

**[SAY]**
> "This is the live dashboard. The top row shows patients at HIGH and MEDIUM risk, alerts
> currently firing, whether today's lab file arrived on time, and end-to-end latency — about
> three seconds from a reading being produced to it landing in the database. Below that,
> per-query throughput and Kafka consumer lag, which sits at zero."

**[DO]** Switch to the terminal.

```bash
curl -s localhost:8000/ward/summary | jq '.tiers, .active_alerts, .vitals.data_lag_sim_minutes'
```

**[SAY]**
> "The same numbers are available through the API — this is the ward summary endpoint."

**[DO]** Run (use your sepsis patient id from setup):

```bash
curl -s localhost:8000/patients/P014 | jq '{risk_tier, ews_score, lab_adjustment, lab_flags}'
```

**[SAY]**
> "And here's one patient's live status — an early-warning score computed from their vitals,
> plus an adjustment from their latest lab results. Spark validates every message, removes
> duplicates, applies a thirty-minute watermark for late data, computes one-hour and four-hour
> windows, and joins in the latest labs — all in the stream."

---

### Scene 4 — Yesterday's labs change the picture (≈3:30–5:00)

**[DO]** Switch to tab 3, the HTML risk report. Scroll to a row where the "with labs" column
differs from "vitals only" (look for the escalation arrow).

**[SAY]**
> "This is the business question, answered directly. For every patient, the daily report shows
> the risk tier from vitals alone, next to the tier after adding yesterday's labs. Here, this
> sepsis patient's vital signs have settled back to normal — low risk on vitals alone — but
> their CRP, lactate, and troponin are still elevated, so the labs raise them to medium risk.
> Their vitals look fine right now; the labs say otherwise."

**[DO]** Switch to tab 4 (Airflow, `lab_ingest` graph), then tab 5 (`daily_risk_report` graph).

**[SAY]**
> "This report is produced daily by Airflow. The lab-ingestion DAG senses the file, validates
> every row, and publishes it into Kafka. The moment it finishes, it triggers the report DAG
> automatically — no polling, no fixed schedule guesswork."

**[DO]** Switch to the terminal.

```bash
make lab-loads
```

**[SAY]**
> "And here's the ledger of every day's file — on time, late, missing, or quarantined for bad
> data — which is also how we guarantee a re-delivered file is never loaded twice."

---

### Scene 5 — Robustness (≈5:00–6:30)

**[DO]** In the terminal, run:

```bash
echo y | make stream-reset
make stream-status
```

**[SAY]**
> "One more thing worth showing: this is a full Kappa replay. We just wiped every derived
> table and checkpoint and rebuilt everything from Kafka from scratch. Every write is an
> upsert on a natural key, so this is safe to do any time — we actually used exactly this
> replay to fix a scoring bug we found while calibrating alert thresholds, without losing any
> history."

*(Wait for `stream-status` to show non-zero row counts before moving on — usually well under a
minute.)*

---

### Scene 6 — Observability and alerts (record separately, ≈75–90 real seconds)

This scene needs real wait time, so record it as its own short clip and speed it up in editing.

**[DO]** Switch to tab 6, Prometheus alerts page (empty/all green). In the terminal:

```bash
docker compose stop vitals-simulator
```

**[SAY]** (say this line, then go quiet and let the recording run)
> "Let's stop the vitals producer entirely and watch the alerting pick it up."

**[DO]** Keep recording the Prometheus alerts tab, refreshing every 15–20 seconds (⌘R), until
`VitalsProducerSilent` goes from pending to firing (typically ~75 seconds).

**[DO]** Restore it:

```bash
docker compose start vitals-simulator
```

**[EDIT LATER]** Speed this clip up 3–4× in your editor so the wait doesn't drag, then cut a
short static freeze-frame on the "firing" state so it's readable at the faster speed.

**[SAY, recorded normally over the sped-up clip in post, or live before speeding up]**
> "There are fifteen alert rules in total, each unit-tested so we know exactly when they should
> and shouldn't fire. The three required by the brief are: no vitals received for two minutes,
> an invalid-record rate above five percent, and the daily lab file missing or late against its
> simulated deadline. Here, the source-side alert fires first — telling an operator it's the
> bedside monitors, not the processing pipeline, that's gone quiet."

---

### Scene 7 — Close (≈6:30–7:00, or append after Scene 6's clip)

**[DO]** Return to tab 1 (the architecture diagram) or a plain desktop/terminal.

**[SAY]**
> "To summarise: a Kappa architecture with one shared scoring implementation; strict validation
> with a dead-letter queue for anything malformed; idempotent, replayable stream processing;
> and observability across every stage, with tested alerts for the failure modes that actually
> matter on a ward. All data here is synthetic, and the risk score is an illustrative,
> simplified version of NEWS2 — not a clinical tool. Thank you."

---

## 4. After recording — assembly checklist

1. Import Take A and Take B into iMovie (or your editor of choice).
2. Trim dead air at the start/end of each clip and between [DO]/[SAY] pairs where you paused
   too long.
3. Speed up Take B (Scene 6) to 3–4×; add a 2-second freeze on the "firing" alert state.
4. Splice Take B in between Scene 5 and Scene 7 of Take A.
5. Watch the full export once, full screen, with headphones — check text is readable and audio
   levels are consistent.
6. Export at 1080p, keep it under the module's stated size/length limit if one exists.

## 5. Full voiceover script (continuous read)

Use this if recording voiceover separately from the screen capture — read it straight through
at a steady pace (aim for ~140 words/minute), pausing about a second between paragraphs so
there's room to cut/align against the screen recording afterward.

> This is a near-real-time monitoring pipeline for hospital ward vital signs. A ward needs to
> know which patients are deteriorating right now, and how yesterday's lab results change that
> picture. Vitals stream in from bedside monitors every second; lab results arrive once a day
> from pathology.
>
> We chose a Kappa architecture: both vitals and lab results flow through Kafka, and a single
> Spark Structured Streaming job computes everything downstream. The deciding reason is
> consistency — there's exactly one scoring implementation, shared by the live API and the
> daily report, so they can never disagree about the same patient. We rejected a Lambda
> architecture because its separate batch layer would duplicate that scoring logic just to
> process around a hundred lab rows a day — infrastructure without a workload.
>
> Here's live traffic on the vitals topic — JSON readings keyed by patient id, so each
> patient's readings stay in order for trend detection. Every simulated patient follows a
> hidden clinical storyline — sepsis, respiratory failure, haemorrhage, or stable — with
> gradual deterioration episodes, sudden spikes, and deliberately broken, late, and duplicate
> messages mixed in. And everything runs on a simulated clock — one simulated day passes every
> five real minutes — used consistently across every component.
>
> This is the live dashboard. The top row shows patients at HIGH and MEDIUM risk, alerts
> currently firing, whether today's lab file arrived on time, and end-to-end latency — about
> three seconds from a reading being produced to it landing in the database. Below that,
> per-query throughput and Kafka consumer lag, which sits at zero. The same numbers are
> available through the API — this is the ward summary endpoint. And here's one patient's live
> status — an early-warning score computed from their vitals, plus an adjustment from their
> latest lab results. Spark validates every message, removes duplicates, applies a
> thirty-minute watermark for late data, computes one-hour and four-hour windows, and joins in
> the latest labs — all in the stream.
>
> This is the business question, answered directly. For every patient, the daily report shows
> the risk tier from vitals alone, next to the tier after adding yesterday's labs. Here, this
> sepsis patient's vital signs have settled back to normal — low risk on vitals alone — but
> their CRP, lactate, and troponin are still elevated, so the labs raise them to medium risk.
> Their vitals look fine right now; the labs say otherwise. This report is produced daily by
> Airflow. The lab-ingestion DAG senses the file, validates every row, and publishes it into
> Kafka. The moment it finishes, it triggers the report DAG automatically — no polling, no
> fixed schedule guesswork. And here's the ledger of every day's file — on time, late, missing,
> or quarantined for bad data — which is also how we guarantee a re-delivered file is never
> loaded twice.
>
> One more thing worth showing: this is a full Kappa replay. We just wiped every derived table
> and checkpoint and rebuilt everything from Kafka from scratch. Every write is an upsert on a
> natural key, so this is safe to do any time — we actually used exactly this replay to fix a
> scoring bug we found while calibrating alert thresholds, without losing any history.
>
> Let's stop the vitals producer entirely and watch the alerting pick it up. There are fifteen
> alert rules in total, each unit-tested so we know exactly when they should and shouldn't
> fire. The three required by the brief are: no vitals received for two minutes, an
> invalid-record rate above five percent, and the daily lab file missing or late against its
> simulated deadline. Here, the source-side alert fires first — telling an operator it's the
> bedside monitors, not the processing pipeline, that's gone quiet.
>
> To summarise: a Kappa architecture with one shared scoring implementation; strict validation
> with a dead-letter queue for anything malformed; idempotent, replayable stream processing;
> and observability across every stage, with tested alerts for the failure modes that actually
> matter on a ward. All data here is synthetic, and the risk score is an illustrative,
> simplified version of NEWS2 — not a clinical tool. Thank you.

## 5b. TTS-ready version (ElevenLabs / AI voice generation)

If you're generating narration with ElevenLabs (or similar) instead of reading it yourself, use
this version rather than §5 above. It's the same script with edits that make it read more
naturally aloud:

- Risk tiers written lowercase (`high`, `medium`) — short ALL-CAPS words are sometimes spelled
  out letter-by-letter instead of read as words.
- `NEWS2` written as "news two" — otherwise it tends to come out "N, E, W, S, two".
- `DAG` written as lowercase "dag" — in data engineering it's pronounced as a word (rhymes with
  "bag"), not spelled out; most TTS engines default to spelling it unless nudged.
- Most em dashes replaced with commas, periods, or a colon, and a few long multi-clause
  sentences split in two — em dashes are the most common source of flat or oddly-timed pauses
  in synthesized speech.

Generate it **paragraph by paragraph** (each blank-line-separated block below lines up with one
Scene from §3) rather than as one long file — pacing stays more consistent, and a bad take only
costs you one short re-generation instead of the whole voiceover.

```
This is a near-real-time monitoring pipeline for hospital ward vital signs. A ward needs to know which patients are deteriorating right now, and how yesterday's lab results change that picture. Vitals stream in from bedside monitors every second; lab results arrive once a day from pathology.

We chose a Kappa architecture: both vitals and lab results flow through Kafka, and a single Spark Structured Streaming job computes everything downstream. The deciding reason is consistency. There's exactly one scoring implementation, shared by the live API and the daily report, so they can never disagree about the same patient. We rejected a Lambda architecture because its separate batch layer would duplicate that scoring logic just to process around a hundred lab rows a day. That's infrastructure without a workload.

Here's live traffic on the vitals topic, JSON readings keyed by patient ID, so each patient's readings stay in order for trend detection. Every simulated patient follows a hidden clinical storyline, sepsis, respiratory failure, haemorrhage, or stable, with gradual deterioration episodes, sudden spikes, and deliberately broken, late, and duplicate messages mixed in. Everything runs on a simulated clock: one simulated day passes every five real minutes, used consistently across every component.

This is the live dashboard. The top row shows patients at high and medium risk, alerts currently firing, whether today's lab file arrived on time, and end-to-end latency, about three seconds from a reading being produced to it landing in the database. Below that, per-query throughput and Kafka consumer lag, which sits at zero. The same numbers are available through the API. This is the ward summary endpoint. And here's one patient's live status: an early-warning score computed from their vitals, plus an adjustment from their latest lab results. Spark validates every message, removes duplicates, applies a thirty-minute watermark for late data, computes one-hour and four-hour windows, and joins in the latest labs, all within the stream.

This is the business question, answered directly. For every patient, the daily report shows the risk tier from vitals alone, next to the tier after adding yesterday's labs. Here, this sepsis patient's vital signs have settled back to normal, low risk on vitals alone, but their CRP, lactate, and troponin are still elevated, so the labs raise them to medium risk. Their vitals look fine right now; the labs say otherwise. This report is produced daily by Airflow. The lab-ingestion dag senses the file, validates every row, and publishes it into Kafka. The moment it finishes, it triggers the report dag automatically, with no polling and no fixed schedule guesswork. And here's the ledger of every day's file: on time, late, missing, or quarantined for bad data. It's also how we guarantee a re-delivered file is never loaded twice.

One more thing worth showing: this is a full Kappa replay. We just wiped every derived table and checkpoint and rebuilt everything from Kafka from scratch. Every write is an upsert on a natural key, so this is safe to do at any time. In fact, we used exactly this replay to fix a scoring bug we found while calibrating alert thresholds, without losing any history.

Let's stop the vitals producer entirely and watch the alerting pick it up. There are fifteen alert rules in total, each unit-tested so we know exactly when they should and shouldn't fire. The three required by the brief are: no vitals received for two minutes, an invalid-record rate above five percent, and the daily lab file missing or late against its simulated deadline. Here, the source-side alert fires first. It tells an operator that it's the bedside monitors, not the processing pipeline, that's gone quiet.

To summarise: a Kappa architecture with one shared scoring implementation. Strict validation with a dead-letter queue for anything malformed. Idempotent, replayable stream processing. And observability across every stage, with tested alerts for the failure modes that actually matter on a ward. All data here is synthetic, and the risk score is an illustrative, simplified version of news two. It is not a clinical tool. Thank you.
```

If "dag" still comes out spelled letter-by-letter, try capitalizing it as `Dag` on its own (no
other nearby caps), or use a pronunciation dictionary if your ElevenLabs plan has one.

## 6. If something goes wrong during recording

- **Spark container restarting:** `make logs s=spark` — it resumes from checkpoint
  automatically; just pause and wait rather than re-recording from scratch.
- **No report file yet:** `make dag-trigger d=lab_ingest` (the report DAG follows via the
  asset within a few seconds).
- **Clock looks paused/wrong:** you (or a previous session) used plain `docker compose down`
  instead of `make down`. Use `make down` / `make up` going forward — they pause and resume
  the simulated clock correctly.
- **An alert won't clear before you need to move on:** cut here and resume recording after
  `docker compose start vitals-simulator` has had ~30 seconds to settle.
