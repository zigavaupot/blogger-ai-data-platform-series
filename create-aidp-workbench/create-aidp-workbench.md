![Setting Oracle AI Data Platform in OCI](https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/setting-aidp.png)

# Setting Up the AI Data Platform Environment

*This post backs up a step from the rest of the [AI Data Platform series](https://zigavaupot.blogspot.com/2026/08/ai-data-platform-series-just-streams.html). Before any bronze/silver/gold Spark job can run, or a stream producer can hand data off to something downstream, the AI Data Platform (AIDP) environment itself has to exist: a Workbench, a workspace, a catalog, compute, and a place to keep secrets. This post walks through building all of that from nothing on a brand-new OCI tenancy, using the same TfL bus-arrivals demo's naming (catalog `tfl`, schemas `bronze`/`silver`/`gold`) as the throughline.*

## The shape of an AI Data Platform environment

AIDP's console groups things into a hierarchy that's worth having straight before clicking anything: a **Workbench** instance (the top-level resource, e.g. `aidp001`) sits on an **Autonomous AI Lakehouse** — which isn't a separate product, just a regular Autonomous Database provisioned with workload type Lakehouse. Inside the Workbench, one or more **Workspaces** (`workspace001` here) each hold their own **Compute** clusters (managed Spark), **Notebooks**, and **Folders**. Separately, at the Workbench level, sits the **Master Catalog** — a Databricks-Unity-Catalog-style `catalog.schema.table` namespace shared across workspaces, with Volumes for anything that isn't a table (streaming checkpoints, mainly).

None of this is needed by the stream producer itself — that only talks to OCI Streaming. It's everything *downstream* of the producer — bronze, silver, gold, and eventually Oracle Analytics — that lives inside this environment.

## Creating the Workbench

**Analytics & AI > AI Data Platform Workbenches > Create AI Data Platform Workbench** is a single page, which is narrower than Oracle's own documentation suggests. It asks for a Workbench name and a default workspace name in one step (no separate "create workspace" screen), a Lakehouse configuration — **Choose existing**, **Create new**, or **None** — and a storage-encryption choice. Picking **Create new** for the Lakehouse only asks for two fields: a fixed `ADMIN` username and a password. There's no ECPU or storage sizing exposed at all; AIDP owns those defaults and provisions the underlying Autonomous AI Lakehouse itself.

The one part worth calling out in detail is the policy step. The wizard's "Add policies" section doesn't ask you to write IAM statements by hand — it shows you the exact ones it's about to create, scoped to a **Standard** access level by default. On a clean tenancy (`smartqcloud`, compartment `shared`), the auto-generated statements all follow the same pattern:

```
allow any-user TO {AUTHENTICATION_INSPECT, DOMAIN_INSPECT, DOMAIN_READ,
  DYNAMIC_GROUP_INSPECT, GROUP_INSPECT, GROUP_MEMBERSHIP_INSPECT,
  USER_INSPECT, USER_READ} IN TENANCY where all
  {request.principal.type='aidataplatform'}

allow any-user to manage buckets in tenancy where all
  {request.principal.type='aidataplatform',
   any {request.permission = 'BUCKET_CREATE', request.permission =
   'BUCKET_INSPECT', request.permission = 'BUCKET_READ',
   request.permission = 'BUCKET_UPDATE'}}

allow any-user to manage log-groups in compartment id <shared-compartment-OCID>
  where ALL {request.principal.type='aidataplatform'}
```

The detail worth noticing: every statement is scoped by `request.principal.type='aidataplatform'`, not by a dynamic group. These are `any-user` grants that only ever fire when the AI Data Platform service itself is the requester — a cleaner model than the dynamic-group dance older OCI services still use, and one less resource to create and maintain by hand.

Click **Add**, then **Create**, and once the instance goes Active you land on the Workbench home page — Recent activity, quick links to enable AI features, get data, and open a notebook:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/workbench-home.png" alt="AI Data Platform Workbench home page for aidp001">
  <figcaption>The Workbench home page once aidp001 is Active.</figcaption>
</figure>

## Building the Master Catalog

With the Workbench up, **Master Catalog > Create Catalog** asks for a catalog name, a type (Standard catalog is the one that matters here), and an optional compartment override — leave it matching the Workbench's own compartment unless you specifically want the catalog's storage elsewhere:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/create-catalog-form.png" alt="Create catalog form in Master Catalog, name tfl, Standard catalog, compartment shared">
  <figcaption>Creating the tfl catalog — Standard catalog, compartment shared.</figcaption>
</figure>

A few seconds later it shows up Active alongside the platform's own `default` catalog:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/master-catalog-tfl-created.png" alt="Master Catalog list showing tfl and default catalogs, both Active">
  <figcaption>tfl created and Active, next to the platform's built-in default catalog.</figcaption>
</figure>

Worth a second look here: the badge next to "Master catalog" reads **Default Cluster (Accepted)**. The Master Catalog has its own dedicated, system-managed Spark compute backing catalog operations — separate from anything you provision yourself — and it starts spinning up the moment you create your first catalog. That becomes relevant a couple of steps later.

Schemas and the checkpoint volume come later, once there's a cluster to run the `CREATE SCHEMA` statements from — Master Catalog's own UI doesn't have a "create schema" button independent of a running notebook.

## Spinning up a Spark cluster

Workspace > **Compute > Create Cluster** offers two paths. **Quickstart** hands you a fixed, fairly generous preset — AMD driver and worker, 2 OCPUs / 32GB each, autoscaling 1-10 workers:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/create-cluster-quickstart-defaults.png" alt="Create cluster form, Quickstart configuration, 2 OCPU/32GB driver and worker, autoscale 1-10 workers">
  <figcaption>Quickstart's defaults — comfortable, but more than a demo actually needs.</figcaption>
</figure>

**Custom** exposes everything: driver/worker shape, OCPUs, memory, block volume size, and whether the worker count is static or autoscaled. For this demo, with a raw TfL poll landing around 15k rows every 10 seconds (well under that after the producer's own dedupe), a single static worker at the smallest available shape is plenty:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/create-cluster-custom-sizing.png" alt="Create cluster form, Custom configuration, driver 1 OCPU/16GB, worker 1 OCPU/16GB/100GB, static 1 worker, named tfl_cluster">
  <figcaption>Right-sized down: 1 OCPU/16GB driver and worker, static single worker, named tfl_cluster.</figcaption>
</figure>

Hit Create, and — notice this — *two* clusters start Creating, not one: `tfl_cluster`, and a second, system-managed **Default Master Catalog Compute** cluster that came along for the ride from the catalog step above.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/compute-clusters-creating.png" alt="Compute list showing tfl_cluster and Default Master Catalog Compute both in Creating state">
  <figcaption>Both clusters provisioning together — tfl_cluster and the platform's own catalog compute.</figcaption>
</figure>

Both eventually settle into Active:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/compute-clusters-active.png" alt="Compute list showing tfl_cluster and Default Master Catalog Compute both Active">
  <figcaption>tfl_cluster (4 active cores) and Default Master Catalog Compute (8 active cores) both Active.</figcaption>
</figure>

One thing not to be alarmed by along the way: the async-operations panel briefly showed the Default Master Catalog cluster's `CREATE_CLUSTER` operation as **Canceled** partway through, before it went on to finish normally. Nothing needed doing about it — it self-resolved into Creating, then Active, on its own.

## Creating the schemas and the checkpoint volume

Schemas get created from SQL, run inside a notebook attached to a live cluster — there's no separate console form for it. **Create > Notebook**, name it (`tfl_setup.ipynb`), and it opens unattached, defaulting to Python:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/notebook-created-no-cluster.png" alt="New notebook tfl_setup.ipynb, no cluster attached, Language Python">
  <figcaption>A fresh notebook — no cluster attached yet, language defaulted to Python.</figcaption>
</figure>

The **Cluster** dropdown lists clusters you can attach to directly:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/notebook-attach-cluster-dropdown.png" alt="Cluster dropdown showing Attach existing cluster option with tfl_cluster listed">
  <figcaption>Attaching the notebook to tfl_cluster via Attach existing cluster.</figcaption>
</figure>

Switch the notebook's language from Python to SQL, then run:

```sql
CREATE SCHEMA IF NOT EXISTS tfl.bronze;
CREATE SCHEMA IF NOT EXISTS tfl.silver;
CREATE SCHEMA IF NOT EXISTS tfl.gold;
CREATE VOLUME IF NOT EXISTS tfl.gold.tfl_volume;
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/notebook-schema-creation-run.png" alt="Notebook cell run with SQL creating three schemas and a volume, status CREATED, cluster tfl_cluster Active">
  <figcaption>All four statements in one cell — the CREATED status shown is for the last one, the volume.</figcaption>
</figure>

A single cell only shows one result table (for the final statement), so the real confirmation is checking Spark's own job history — Stage SUCCESS, one task, no errors:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/spark-ui-job-succeeded.png" alt="Spark UI Job 1 details, status SUCCEEDED, one completed stage">
  <figcaption>Spark's own UI confirms the job succeeded end to end.</figcaption>
</figure>

— and, more directly, going back to Master Catalog and looking at what's actually there:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/catalog-tfl-schemas-active.png" alt="tfl catalog Schemas tab listing gold, silver, bronze, and default schemas, all Active">
  <figcaption>tfl now has bronze, silver, and gold — plus a default schema the platform creates on its own.</figcaption>
</figure>

`gold.tfl_volume` shows up the same way, empty and Active, ready for the silver layer's streaming checkpoint later in the series.

## Vault and secrets for the Kafka credentials

The old version of this demo read its OCI Streaming credentials out of OCI Vault via an `aidputils.secrets.get(...)` helper, and the same approach carries over here — with one console change worth flagging up front: secrets have moved out of the classic Vault UI into a dedicated **Secrets Management** console under Identity & Security. Docs and blog posts describing secrets as a tab inside Vault are describing an older layout.

Rather than reuse a vault that already backs other workloads in the tenancy, this gets its own: **Identity & Security > Vault > Create Vault**, named `tfl-aidp-vault`, in the `shared` compartment:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/vault-tfl-aidp-vault-active.png" alt="tfl-aidp-vault detail page, Active, compartment shared, with cryptographic and management endpoints">
  <figcaption>A dedicated vault for this project, kept separate from other workloads' vaults in the same compartment.</figcaption>
</figure>

A vault needs a Master Encryption Key before anything can be encrypted with it — **Master encryption keys > Create Key**:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/create-master-key-form.png" alt="Create Key form, name tfl-aidp-key, AES algorithm, 256-bit length">
  <figcaption>tfl-aidp-key: AES, 256-bit.</figcaption>
</figure>

One correction against the console worth noting: OCI Vault keys default to **HSM** protection mode (hardware-backed), not Software — the key list confirms it as Enabled, HSM, AES:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/master-encryption-keys-list.png" alt="Master encryption keys list showing tfl-aidp-key, Enabled, HSM protection mode, AES algorithm">
  <figcaption>Note the terminology: vaults show Active, but keys show Enabled.</figcaption>
</figure>

With the vault and key in place, **Identity & Security > Secret Management** is where the actual secrets get created — starting empty, scoped to the vault:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/secrets-management-empty.png" alt="Secrets Management console, empty, filtered to compartment shared and vault tfl-aidp-vault">
  <figcaption>The new Secrets Management console — separate from the Vault UI, filtered by compartment and vault.</figcaption>
</figure>

**Create Secret** matters in one specific way: the form defaults to **Automatic secret generation** (a randomly generated passphrase), which is the wrong choice here — `KAFKA_USERNAME` and `KAFKA_PASSWORD` are specific, known-shape values (a compound OCI Streaming username, and an OCI Auth Token), not something the console should invent. Switching to **Manual secret generation** exposes a plain text box instead:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/create-secret-kafka-usr-manual.png" alt="Create secret form for tfl-kafka-usr, Manual secret generation, Plain-Text template, placeholder contents">
  <figcaption>tfl-kafka-usr — Manual generation, Plain-Text, placeholder content pending the real stream pool.</figcaption>
</figure>

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/create-secret-kafka-pwd-manual.png" alt="Create secret form for tfl-kafka-pwd, Manual secret generation, Plain-Text template, placeholder contents">
  <figcaption>tfl-kafka-pwd — same approach, placeholder pending a real OCI Auth Token.</figcaption>
</figure>

Both secrets exist as placeholders at this point, deliberately — provisioning the actual OCI Streaming pool and generating a real Auth Token belongs to this series' next stage, not to environment setup. OCI Vault versions secrets, so swapping in real values later is a new secret *version*, not a delete-and-recreate:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/create-aidp-workbench/images/secrets-list-active.png" alt="Secrets list showing tfl-kafka-usr and tfl-kafka-pwd, both Active">
  <figcaption>Both secrets Active, holding placeholder values until the stream pool exists.</figcaption>
</figure>

## What's next

With the Workbench, catalog, compute, and a place to keep secrets all in place, the environment side of this series is done. The remaining piece — provisioning the actual OCI Streaming pool and topic, generating a real OCI Auth Token, and updating these two secrets with real values — picks back up in the stream-producer post, where the rebuilt `tfl_stream_producer.py` actually starts publishing.

*Related: [Just Streams: Real-Time Data Pipelines on OCI](https://zigavaupot.blogspot.com/2026/08/ai-data-platform-series-just-streams.html) (series intro)*
