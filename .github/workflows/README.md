# CI/CD for the Databricks Asset Bundle

Two GitHub Actions workflows drive this bundle (`databricks.yml`):

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `validate.yml` | Pull request to `main` | `databricks bundle validate --target dev` — proves the bundle is well-formed before merge, then posts the resolved `bundle summary` as a PR comment. No deploy. |
| `deploy.yml` | Push to `main` → **dev**; tag `v*` → **prod**; manual dispatch (pick target) | `bundle validate` + `bundle deploy` for the chosen target, then optionally `bundle run` the medallion job on prod. |
| `destroy.yml` | Manual dispatch only (typed confirmation) | `databricks bundle destroy` for the chosen target — tears down deployed jobs/pipelines/dashboards. |

Both targets (`dev`, `prod`) live in the same workspace but land in separate
schemas (`sdp_workshop` vs `sdp_workshop_prod`), so this demonstrates the
dev → prod promotion story from a single repo.

## One-time setup

### 1. Create a service principal (OAuth M2M)

Deployments authenticate as a Databricks **service principal**, not a personal
token. In the workspace: **Settings → Identity and access → Service principals**,
create one, then generate an **OAuth secret** (client ID + client secret).

Grant it whatever the bundle needs to deploy: `CAN_MANAGE` on the target
catalog/schema, cluster/pipeline creation, and job run permissions.

### 2. Add GitHub secrets

Store these as **Environment secrets** (Settings → Environments → `dev` / `prod`)
so prod credentials stay separate from dev:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | `https://fevm-finclear-sdp-demo.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | Service-principal OAuth client ID |
| `DATABRICKS_CLIENT_SECRET` | Service-principal OAuth client secret |

The Databricks CLI picks up `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`
automatically for OAuth M2M auth — no `databricks configure` step needed.

### 3. Protect the `prod` environment

In **Settings → Environments → prod**, add a **Required reviewers** rule.
GitHub then pauses `deploy.yml` for manual approval before it touches prod —
the human gate in the promotion flow.

## Promotion flow

```
open PR ──▶ validate.yml (dev validate)
   │
   merge to main ──▶ deploy.yml ──▶ deploy DEV
   │
   tag v1.0.0 ─────▶ deploy.yml ──▶ [prod reviewer approves] ──▶ deploy PROD ──▶ run medallion job
```

## Try it locally first

The workflows just run the same CLI you use by hand:

```bash
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run finclear_medallion --target dev
```
