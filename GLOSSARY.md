# pg_aqueduct Ubiquitous Language

This is the shared vocabulary for `pg_aqueduct`. It describes the objects, states, changes, and operational decisions that appear in the CLI, migration files, planner, catalog, and documentation. The words matter because a stream-table system has several versions of the truth at once: what a team has declared in Git, what PostgreSQL is running now, and what `pg_aqueduct` remembers about earlier applies.

Use these definitions when naming features, writing diagnostics, reviewing plans, and explaining incidents. A term in this document has a narrower meaning than it might have in a general database conversation. In particular, `pg_aqueduct` evolves a graph of incrementally maintained tables. It does not own every table in the database, and it does not treat a migration file as an imperative script to run from top to bottom.

## Scope And Shape

### Aqueduct

`aqueduct` is the command-line tool and `aqueduct-core` is the library behind it. Together they turn a desired stream-table definition into a typed diff, classify each change by risk and cost, and execute an ordered plan against one PostgreSQL target. The name refers to the migration system as a whole, while `pg_aqueduct` is the project and repository name.

Aqueduct sits between a version-controlled project and the `pg_trickle` catalog in a live database. It owns the migration lifecycle, the recorded history, and the safety checks around that lifecycle. It does not replace a relational schema migration tool for ordinary application tables, a model builder such as dbt, or a monitoring system.

### Project

A project is the unit of ownership and migration history in `pg_aqueduct`. It is named in `aqueduct.toml`, owns a set of stream tables and related objects, and gets its own entries in the Aqueduct catalog for versions, migrations, locks, and ownership.

The project boundary is also a safety boundary. When `aqueduct destroy` checks ownership, it is asking whether an object belongs to this project rather than assuming that every similarly named stream table is safe to remove. Multiple projects can share a database when their ownership and names remain explicit.

### Target

A target is a named PostgreSQL environment declared in `aqueduct.toml`, such as `dev`, `staging`, or `prod`. It supplies the connection details and any environment-specific settings needed to compare or change one database.

The same desired project can be evaluated against several targets. A target is not another copy of the migration files and it is not a version number. It is the database context in which desired state, live state, and recorded history are compared.

### DAG

A DAG is the directed acyclic graph formed by stream-table dependencies. Each node represents a stream table, and each directed edge means that one node reads from another object upstream. Sources and consumer views sit at the edges of this graph, but the stream-table nodes are the part Aqueduct evolves.

The graph must remain acyclic because a refresh dependency cannot wait on itself. Its shape determines migration order: an upstream change can affect every downstream node that reads it, while a downstream change can often be handled without touching its parents. The graph is why a seemingly small SQL edit can require more care than a standalone table alteration.

### Node

A node is one stream table in the DAG, identified by a fully qualified name and described by its query, refresh mode, schedule, CDC mode, and dependencies. The planner creates a `NodeDelta` for a node when it compares the desired and actual definitions.

People sometimes use "node" to mean any object in a data pipeline. In this project, keep the narrower meaning: a node is a stream-table node unless the surrounding text explicitly says source or consumer view. This keeps plan output and incident discussions precise.

### Source Table

A source table, also called a base table, is a table that a stream-table query reads from without being a derived stream-table node. It may be an existing application table that Aqueduct only tracks, or it may be declared as an owned source whose DDL Aqueduct is allowed to manage.

A source table is the beginning of a dependency path. A change to an owned source can produce cascade impacts on every stream table whose query references it. Aqueduct records that impact so the plan shows why downstream work is required instead of presenting a list of unrelated rebuilds.

### Dependency, Upstream, And Downstream

A dependency is an object that a stream table needs in order to evaluate its query. Aqueduct can read dependencies from SQL `FROM` and `JOIN` references and can also accept explicit `@aqueduct:depends_on` declarations for cases that inference cannot see.

Upstream means closer to a source; downstream means closer to a consumer. If `daily_totals` reads from `orders`, then `orders` is upstream of `daily_totals`, and `daily_totals` is downstream of `orders`. These words describe graph direction, not execution speed or ownership.

### Stream Table

A stream table is a materialized query maintained by `pg_trickle`. It stores the current result of a query while retaining the machinery needed to update that result as its inputs change. In Aqueduct, a stream table is the primary object being created, altered, rebuilt, or dropped.

A stream table has more than a SQL body. Its refresh mode, schedule, CDC mode, qualified name, and dependency set are part of its specification. A plan can therefore change a stream table without changing its query, such as when a schedule changes, and the classifier can choose a cheaper operation for that case.

### Consumer View

A consumer view is a stable database view exposed to applications, dashboards, or other readers. It points at a stream table, and it may apply a projection or filter of its own. Consumer views are optional, but they provide an important indirection layer when the underlying DAG needs to change without moving the name that readers use.

During a blue/green deployment, Aqueduct builds a new green graph and atomically changes the consumer views to point at it. The consumer sees one stable view name while the implementation behind that name changes. A consumer view is therefore an API boundary, not just a convenient alias.

### Qualified Name

A qualified name identifies a database object as `schema.name`, for example `public.order_totals`. Aqueduct uses qualified names when matching desired objects to live objects, inferring dependencies, and generating plan steps.

The schema is part of the identity. Two tables with the same unqualified name in different schemas are different nodes for planning purposes, even if a SQL search path would make them look interchangeable. Writing the qualified form removes that ambiguity from plans and operational commands.

## State And Records

### Desired State

Desired state is the complete `DagState` assembled from the project files. It includes the stream tables, declared or owned source tables, and managed consumer views that the project says should exist at a target.

Desired state is declarative. Aqueduct reads the directory as a specification and compares the resulting whole picture with the database. A file rename, a removed file, or a changed front-matter directive can therefore describe a create, drop, or alteration even when no file contains an imperative `ALTER` statement.

### Live State

Live state, also called actual state, is what Aqueduct reads from the target database and the `pg_trickle` catalog. It describes the objects that exist now, their current definitions, and the metadata available for comparison.

Live state is not assumed to be correct just because PostgreSQL accepted it. Someone may have changed a stream table out of band, a deployment may have stopped halfway through, or the target may be running an older project version. The planner compares live state with desired state and recorded history so those cases become visible as deltas or drift.

### Recorded History

Recorded history is the set of snapshots and migration-run records stored in the Aqueduct catalog. It answers questions that neither files nor the live database can answer alone, such as which desired specification was applied last, which steps completed before a crash, and which earlier DAG version is available for rollback.

History is evidence, not a substitute for inspection. An applied version can be followed by an out-of-band database change, and a failed migration can leave a target between versions. That is why planning always considers the current live state as well as the recorded records.

### Catalog

The catalog is Aqueduct's metadata store inside PostgreSQL. By default it lives in the `aqueduct` schema and contains DAG versions, migration runs, project locks, ownership records, consumer-view records, and blue/green deployment records.

The catalog does not hold the stream-table data itself. It records the information Aqueduct needs to plan, resume, audit, and protect changes to that data. A catalog entry is about the migration system's understanding of a target, not a replacement for the `pg_trickle` runtime catalog.

### Catalog Schema

The catalog schema is the PostgreSQL schema where Aqueduct stores its own metadata. The default is `aqueduct`, but a project can configure another validated schema name when several installations need clearer separation.

This term is easy to confuse with the schemas that contain stream tables. A catalog schema contains Aqueduct's records; a stream-table schema contains the database objects being evolved. Changing the catalog schema changes where Aqueduct looks for its metadata, not where a stream table lives.

### DAG Version

A DAG version is an immutable snapshot of the desired DAG specification recorded after a successful apply. It has a numeric version, the project name, the specification hash, the serialized specification, the plan, and application metadata.

A DAG version is a point in the project's recorded history, not necessarily a Git commit and not a PostgreSQL transaction ID. Rollback chooses a prior DAG version as its destination, then computes a new forward plan from the target's current state to that destination.

### Spec Hash

A spec hash is the SHA-256 fingerprint of a desired DAG specification. Aqueduct puts it into plans and recorded snapshots so an exported plan can be checked against the migration files that exist when someone applies it.

The hash protects the meaning of a plan, not the connection to a particular database. If the migration files change after `aqueduct plan --out`, `aqueduct apply --plan` can reject the stale artifact instead of applying steps calculated for a different desired state.

### Drift

Drift is a difference between the live database and the state Aqueduct expects or last recorded. It can come from an out-of-band DDL change, an interrupted apply, manual repair, or a target that was never brought to the current project version.

Drift is a fact to investigate, not automatically proof of corruption. The useful question is which boundary moved: desired files, live objects, or recorded history. `aqueduct diff` and `aqueduct status` expose the difference, while `aqueduct plan` determines the work needed to reconcile it.

## Files And Lifecycle

### Migration File

A migration file is a version-controlled SQL file that contributes one object specification to desired state. Stream-table files normally live under `migrations/streams`, source files under `migrations/sources`, and consumer files under `migrations/consumers`, although the project configuration can shape the layout.

The file is declarative even though its body contains SQL. Aqueduct parses its front matter and SQL body into a typed specification; it does not replay the file as a hand-written sequence of changes. The filename becomes part of the object's identity, so renaming a file can be interpreted as dropping one object and creating another unless the project has a deliberate migration pattern for that change.

### Migration Kind

Migration kind says what a migration file describes: a `stream`, a `source`, or a `consumer`. The kind determines which typed object Aqueduct builds and which part of the database lifecycle it can affect.

A stream file describes a materialized query, a source file describes a base table that may be tracked or owned, and a consumer file describes a stable view. Do not use "migration kind" to mean Free, In-place, Rebuild, or Blue/green. Those are migration classes, which describe the cost and technique for a change.

### Front Matter

Front matter is the set of `-- @aqueduct:` directives at the top of a migration file. It supplies metadata such as `kind`, `schema`, `refresh_mode`, `schedule`, `cdc_mode`, `depends_on`, `owned`, `source`, and `expose_as` without hiding that metadata in a separate configuration file.

Front matter is part of desired state and therefore part of the plan's spec hash. A change to a directive can be a real migration even when the SQL query below it is untouched. Keep directives explicit when they affect graph identity, ownership, refresh behavior, or the name seen by consumers.

### Diff

A diff is the structured comparison between desired state and live state. It contains node deltas for stream tables, source deltas with cascade impacts, and consumer deltas for managed views.

A diff describes what is different; it does not yet say exactly how to fix it. Classification and planning use the diff to choose operations, order them by dependency, and add the safety steps needed around them. An empty diff means the compared states agree, not that the target has no historical migrations.

### Delta

A delta is the change record for one object. A `NodeDelta` can be a create, drop, query alteration, schedule alteration, CDC alteration, metadata alteration, or unchanged result. Source and consumer deltas use their own change kinds because their safety rules are different.

A delta is deliberately smaller than a plan. It identifies the changed object and the before-and-after specifications; it does not include locks, backfills, refresh waits, view swaps, or cleanup. Those execution details belong to plan steps.

### Cascade Impact

A cascade impact is the downstream consequence of changing a source table. It names a stream table that reads from the changed source and records the migration class Aqueduct expects to use for that dependent node.

The word cascade describes dependency analysis, not a blind database `CASCADE` clause. Aqueduct can show that a source change requires downstream rebuild work without automatically dropping every dependent object. Destructive database cascading remains an explicit operation with its own safeguards.

### Plan

A plan is the typed, ordered set of actions Aqueduct proposes after comparing desired and live state and classifying the changes. It includes the project, source and destination versions, summary counts, format version, creation time, spec hash, and executable plan steps.

Planning is the point at which Aqueduct turns language into consequences. The plan should make clear whether a change is metadata-only, preserves materialized state, requires a full rebuild, or needs a parallel graph. `aqueduct apply` computes a plan before it executes one, even when the user does not ask to print it.

### Plan Step

A plan step is one executable unit inside a plan. Examples include acquiring the project lock, validating a query, creating or altering a stream table, backfilling data, waiting for refresh, swapping consumer views, recording a snapshot, and releasing the lock.

Plan steps are smaller than migration classes because one class can require several operations. A Rebuild may include pause, detach, drop, recreate, backfill, reattach, and resume steps. The step-level record is what lets an interrupted apply resume or identify the exact operation that needs a forced retry or skip.

### Migration Run

A migration run is one attempt to execute a plan against a target. Aqueduct records it in `aqueduct.migrations` with its project, source and destination versions, status, plan, progress, and timestamps.

This is the meaning of "migration" in operational records and commands such as `aqueduct apply` or `aqueduct rollback`. It is different from a migration file, which is an input to desired state, and different from a DAG version, which is a successful snapshot in history.

### Apply

Apply is the operation that executes a plan against a target database. It acquires the project lock, performs the steps in order, checkpoints progress, and records a new DAG version when the run succeeds.

Apply is intentionally more than sending SQL to PostgreSQL. It validates the plan against the current specification, watches for failures, handles compensating work where the executor supports it, and leaves records that make a partial run diagnosable. `--dry-run` plans the same work without changing the target.

### Promotion

Promotion moves a validated project state from one named environment to another, such as from `dev` to `staging`. It uses the same declarative files and planning rules as apply, but adds a source-environment check so a known-good state is not promoted from a dirty source by accident.

Promotion is an environment workflow, not a special kind of SQL change. The destination still receives a plan calculated for its own live state, because a target can have different versions, capabilities, or drift even when both environments use the same project files.

### Rollback

Rollback changes the target toward an earlier recorded DAG version. It does not rewind the database by magic or restore a PostgreSQL transaction; it computes and executes a new forward plan from the current state to the selected historical specification.

For Free and many In-place changes, the path can preserve existing materialized data. Rebuild-class work may have already replaced that data with a full refresh, so Aqueduct tracks a lossless window and requires explicit acknowledgement when a rollback can no longer avoid data loss.

### Project Lock

The project lock is the catalog-backed guard that serializes concurrent apply, rollback, promote, or other mutating operations for one project. It carries a holder identity, acquisition time, time-to-live, and paused-node information needed by recovery paths.

The lock protects the migration protocol, not every query in the database. Readers can continue to use stream tables while a compatible change runs, but two Aqueduct processes must not independently mutate the same project and interleave their snapshots or plan steps.

## Change Classes And Refresh

### Migration Class

A migration class is Aqueduct's safety and cost classification for a change. The classifier uses the delta and the available evidence to choose the least disruptive technique it can prove is safe. When it cannot prove that materialized state can be preserved, it chooses Rebuild.

The four classes are Free, In-place, Rebuild, and Blue/green. A class is not a severity label and it is not merely an estimate of elapsed time. It tells the executor what kind of state transition the change requires and tells the operator what kind of risk to expect.

### Free

A Free change affects metadata without changing the stored query result or requiring a rebuild. A schedule change, a CDC mode change, and supported refresh-mode metadata changes normally fit here and can be handled with a single `alter_stream_table` operation.

Free does not mean that an operation is invisible or that it cannot fail. The target still needs the relevant `pg_trickle` capability, the project lock still applies, and the change is still recorded in the migration history. It means Aqueduct has no reason to rewrite the materialized result.

### In-Place

An In-place change updates a stream table while preserving its existing materialized state. Typical examples include adding a passthrough or aggregate column when the structural parts of the query remain compatible, followed by a targeted incremental backfill.

In-place is a proof obligation, not a promise based on the size of the SQL diff. Aqueduct checks the query structure and falls back to Rebuild for changes such as a renamed expression, reordered output, changed grouping, changed join structure, or any other case it cannot establish as safe.

### Rebuild

A Rebuild replaces a stream table's materialized result by dropping or recreating the object and performing a full refresh. It is the conservative answer for structural query changes, including changed grouping keys, join conditions, predicates, and transitions that require new differential state.

A rebuild can be correct and still be operationally expensive. It may need a maintenance window, a temporary mode change, policy or CDC attachment handling, and an explicit allow flag. The class exists to make that cost visible before apply begins, not to make rebuilds look like ordinary alters.

### Blue/Green

Blue/green is a migration class and deployment strategy for structural changes that should avoid replacing the live graph in place. Aqueduct builds a parallel green schema and graph, waits for the green nodes to converge, atomically switches consumer views from blue to green, and retires the old blue schema after its retention period.

Blue and green describe successive implementations of the same consumer-facing contract. The consumer view name stays stable while its target changes. This strategy depends on managed consumer views; without that indirection, readers would still be coupled directly to the schema being replaced.

### Refresh Mode

Refresh mode describes how a stream table receives updated results. Aqueduct supports `DIFFERENTIAL`, `FULL`, and `IMMEDIATE`, and the mode is part of the stream-table specification rather than an incidental runtime setting.

A mode change can alter the migration class. Moving from `DIFFERENTIAL` to `FULL` can be metadata-only, while moving from `FULL` to `DIFFERENTIAL` requires Aqueduct to establish differential-tracking state and is therefore classified conservatively as Rebuild. `IMMEDIATE` means refresh happens synchronously in the triggering transaction and needs special pause and resume handling around rebuilds.

### Differential Refresh

Differential refresh updates a stream table from changes in its inputs instead of recomputing the entire result. It preserves the advantage that makes a stream-table DAG useful: a small input change can produce a small amount of maintenance work even when the result is large.

Differential state is part of what a destructive migration can lose. A rebuild that recreates a table must establish that state again, which is why Aqueduct treats transitions into differential mode and structural query changes with care.

### Full Refresh

A full refresh recomputes a stream table's result from its inputs. It is a normal and sometimes necessary operation, but it reads and rebuilds the full result rather than applying only the latest deltas.

Full refresh is the data-producing part of a Rebuild. It can take a maintenance window on a large table, and once it has replaced the previous materialized state, a later rollback may need the operator to accept data loss or another full rebuild.

### Immediate Refresh

Immediate refresh updates the stream table synchronously within the transaction that changes its input. This gives readers transactionally current derived data, but it also means a rebuild cannot casually drop and recreate the table while writes are flowing through it.

For a Rebuild, Aqueduct can temporarily pause an IMMEDIATE table by switching it to DIFFERENTIAL, perform the required work, and resume IMMEDIATE mode afterward. The pause and resume are part of the plan so the operational compromise is visible and recoverable.

### Schedule

A schedule is the interval or scheduling expression that tells `pg_trickle` when a non-immediate stream table should refresh. It belongs to stream-table metadata and can often change as a Free migration because changing the interval does not change the query or stored result by itself.

A schedule is not a maintenance window. The schedule controls normal refresh cadence for one stream table; a maintenance window controls when an operator permits disruptive migration work for a target or project. Keeping these separate avoids treating routine refresh policy as an outage policy.

### CDC Mode

CDC mode selects how a stream table receives or tracks change data, when the target's `pg_trickle` installation supports that mode. It is stream-table metadata and can often be changed without rebuilding the query result.

Some CDC modes carry external resources, such as a logical replication slot or a `pg_tide` outbox attachment. When a rebuild affects those resources, the plan includes the required detach, drop, create, or reattach steps. The mode name alone does not tell the whole operational story; its attachments matter too.

### Backfill

A backfill populates or repairs materialized rows after a structural or additive change. An In-place backfill is targeted and incremental where possible; a Rebuild backfill follows recreation and generally uses a full refresh.

Backfill is not the same as a refresh schedule. It is a migration step with a defined place in the plan, progress tracking, and often a row-count or duration estimate. Calling it out separately helps operators understand where the work and elapsed time will occur.

### Convergence

Convergence is the point at which a newly built or changed stream table has caught up to the data it should represent. Blue/green plans wait for green nodes to converge before switching consumer views, so the swap does not expose an unfinished result.

Convergence is about data freshness and correctness, not merely object existence. A green table can be created successfully and still be unsuitable for consumers until its refresh lag is within the plan's allowed threshold.

### Maintenance Window

A maintenance window is the declared period during which disruptive work, especially Rebuild-class work, may run. Aqueduct uses it to prevent an otherwise valid plan from unexpectedly performing a full refresh during peak traffic.

The window is an operational guard, not a classification. A change remains Rebuild whether or not the current time is inside the window. Operators can override the guard explicitly when the situation warrants it, and that decision should remain visible in the command history.

### Lossless Window

The lossless window is the period during which a prior materialized state remains available enough for Aqueduct to roll back without accepting data loss. It is most important for Rebuild-class migrations, which can replace the old result during a full refresh.

The window is not a guarantee that every rollback is cheap. It is a boundary around what state the executor can still preserve or recover. Once it has passed, `aqueduct rollback` requires an explicit `--accept-data-loss` acknowledgement for the affected work.

## Ownership And Safety

### Ownership

Ownership records which Aqueduct project manages a stream table. The catalog uses it to prevent one project from accidentally dropping or altering a table registered to another project, especially during destructive commands.

Ownership is separate from PostgreSQL privileges. A role may have permission to issue `DROP TABLE` and still fail an Aqueduct ownership check. The database privilege answers "can this role do it?"; ownership answers "does this project have the right to do it?"

### Tracked Source

A tracked source is a source table that Aqueduct includes in dependency and cascade analysis but does not own for DDL purposes. This is the normal choice for application tables managed by another migration system.

Tracking lets Aqueduct explain how a source change affects the stream DAG without pretending to control the source table's lifecycle. The boundary is intentional: the application schema tool changes the source, and Aqueduct plans the derived-data consequences.

### Owned Source

An owned source is a source table whose declaration gives Aqueduct permission to manage its DDL as part of the project. Its migration file can contain the SQL needed to create or alter the source, subject to the project's permissions and safety rules.

Ownership should be used only when the project is genuinely responsible for the source table. Marking an application-owned table as owned would blur the boundary between base-table schema management and stream-table migration, and could make a cascade plan more destructive than intended.

### Compensating Step

A compensating step is a recovery action recorded for a migration operation that needs cleanup or restoration if a later step fails. Examples include restoring an attachment, undoing a partial blue/green transition, or finishing catalog bookkeeping after a crash.

Compensation is not the same as a database transaction rollback. Some stream-table operations and refreshes outlive a single SQL transaction, so Aqueduct records enough progress to recover deliberately rather than claiming that every failure can be erased atomically.

### Import

Import bootstraps a project from an existing `pg_trickle` deployment. It reads the live stream-table definitions and writes migration files that describe the discovered desired state, allowing a team to bring an already-running DAG under versioned management.

Import is a starting point, not a migration run. After import, the generated files become the project's source of truth and normal plan, diff, apply, and promotion workflows take over. A subsequent plan should be empty when the imported files accurately describe the target.
