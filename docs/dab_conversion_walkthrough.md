# Interactive walkthrough: turn a pipeline into a Databricks Asset Bundle

The goal of this walkthrough is to show FinClear how a pipeline they've prototyped — in the
workspace UI or as notebooks — becomes a **version-controlled, promotable Asset Bundle (DAB)**.
This is the Week-4 DevOps / CI-CD story: one command deploys the whole thing, and dev→prod is a
target flag, not a manual rebuild.

Run these in a terminal with the Databricks CLI authenticated (`-p finclear-sdp`).

---

## Option A — you already built a pipeline in the UI: **generate** the bundle from it

This is the "convert an existing pipeline to a DAB" path, and it's the most compelling to show
live. `databricks bundle generate` reverse-engineers an existing pipeline into bundle source +
a resource YAML.

```bash
# 1. Start an empty bundle
databricks bundle init default-python   # or hand-write databricks.yml (see this repo)

# 2. Find the pipeline id you built in the UI
databricks pipelines list-pipelines -p finclear-sdp

# 3. Generate the resource YAML + pull the source files into the bundle
databricks bundle generate pipeline \
  --existing-pipeline-id <PIPELINE_ID> \
  -p finclear-sdp
#   → writes resources/<name>.pipeline.yml and downloads the pipeline's source files

# 4. Deploy it back as a bundle
databricks bundle validate -p finclear-sdp
databricks bundle deploy   -p finclear-sdp
```

Now the pipeline they clicked together in the UI is code: reviewable, diffable, and promotable.

---

## Option B — author as code from the start (what this repo does)

```
databricks.yml                     # bundle name, variables, dev/prod targets
resources/*.yml                    # pipeline, job, dashboard definitions
pipeline/transformations/*.sql     # the medallion SQL
```

```bash
databricks bundle validate -p finclear-sdp
databricks bundle deploy   -p finclear-sdp          # dev target (default)
databricks bundle run finclear_sdp -p finclear-sdp  # run the pipeline
```

Key ideas to point out in `databricks.yml`:

- **Variables** (`catalog`, `schema`, `volume`, `warehouse_id`) — retarget without touching SQL:
  ```bash
  databricks bundle deploy -p finclear-sdp --var="catalog=finclear_prod,schema=medallion"
  ```
- **Targets** — `dev` (isolated, development mode) and `prod` (separate schema). Promotion is
  just a flag:
  ```bash
  databricks bundle deploy --target prod -p finclear-sdp
  ```
- **One bundle, many resources** — the pipeline, the generator job, and the AI/BI dashboard all
  deploy together and stay in sync.

---

## The DevOps punchline for FinClear

- The bundle is **plain files in Git** → pull requests, review, history, rollback.
- `dev` and `prod` differ only by **target** → no environment drift, no hand-copying.
- A CI job runs `databricks bundle deploy --target prod` on merge to `main` → PR-based promotion,
  exactly the dev→prod workflow they asked about.

> This maps directly onto FinClear's stated dev/prod isolation and PR-based code-promotion goals.
