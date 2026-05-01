---
title: "gather-step configuration reference"
description: "Complete schema reference for gather-step.config.yaml. Covers all fields, defaults, validation rules, depth levels, and workspace-local generated paths."
---

The `gather-step.config.yaml` file declares which repositories belong to the workspace and controls how they are indexed. The file must exist before running `index`, `watch`, or any analysis command. Run `gather-step init` to generate a starting config by auto-discovery.

## Default paths

Given `--workspace /path/to/workspace`, the tool resolves these paths automatically. Override individual paths with the corresponding command flags.

| Purpose | Default path |
|---|---|
| Config | `/path/to/workspace/gather-step.config.yaml` |
| Registry | `/path/to/workspace/.gather-step/registry.json` |
| Storage root | `/path/to/workspace/.gather-step/storage` |
| Graph store | `/path/to/workspace/.gather-step/storage/graph.redb` |
| Metadata store | `/path/to/workspace/.gather-step/storage/metadata.sqlite` |

## Canonical example

The example below uses all optional fields. In practice, most workspaces need only the `repos` block and optionally `indexing.workspace_concurrency`.

```yaml
# gather-step.config.yaml

repos:
  - name: backend_standard
    path: apps/backend_standard
    depth: full
  - name: frontend_standard
    path: apps/frontend_standard
    depth: level2
  - name: shared_contracts
    path: packages/shared_contracts

allow_listed_repos:
  - shared_contracts

github:
  owner: example-org
  api_base_url: https://api.github.com
  token_env: GITHUB_TOKEN

jira:
  project_key: ENG
  base_url: https://example.atlassian.net
  token_env: JIRA_API_TOKEN

indexing:
  exclude:
    - node_modules
    - dist
    - "*.min.js"
    - "*.map"
    - "*.lock"
    - "*.d.ts"
  language_excludes:
    - language: typescript
      patterns:
        - "*.generated.ts"
  include_languages:
    - typescript
    - javascript
  include_dotfiles: false
  min_file_size: 100B
  max_file_size: 1MB
  workspace_concurrency: 4

deployment:
  include:
    - "**/Dockerfile"
    - "**/docker-compose*.yml"
    - "k8s/**/*.yaml"
  gitops_roots:
    - platform/apps
  env_files:
    - .env.example
```

## Top-level fields

### `repos`

**Required.** A list of repository entries. At least one must be present. The config is rejected if `repos` is empty.

Each entry in the list has the following structure:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Logical repo name used in all output, registry entries, and tool responses. Must be non-empty. Must not contain `/`, `\`, newlines, or the special segments `.` and `..`. Must be unique within the config. |
| `path` | string | yes | Repo root path relative to the config file's directory. Must be a relative path. Must not escape the config root via `..` segments. Must not overlap with another repo's path. |
| `depth` | enum | no | Per-repo depth override. Accepts `level1`, `level2`, `level3`, or `full`. When omitted, the value from `--depth` (if passed on the CLI) is used; otherwise defaults to `full`. |

Repo paths that resolve through symlinks are rejected at index time.

---

### `allow_listed_repos`

**Optional.** Default: `[]`.

A list of repo names from `repos` that are granted elevated trust within the workspace. Currently stored in the config model and validated against the `repos` list. Every name in `allow_listed_repos` must appear in `repos`; duplicate names are rejected.

---

### `github`

**Optional.** GitHub integration settings. When provided, future releases may use this block for GitHub-sourced metadata such as pull request history.

| Field | Type | Required | Description |
|---|---|---|---|
| `owner` | string | yes | GitHub organization or user name. |
| `api_base_url` | string | no | GitHub API base URL. Defaults to `https://api.github.com` when absent. |
| `token_env` | string | no | Name of the environment variable that holds the GitHub personal access token. |

---

### `jira`

**Optional.** Jira integration settings. Stored for future use in linking commits to issue tracking.

| Field | Type | Required | Description |
|---|---|---|---|
| `project_key` | string | yes | Jira project key, e.g. `ENG`. |
| `base_url` | string | no | Jira instance base URL, e.g. `https://example.atlassian.net`. |
| `token_env` | string | no | Name of the environment variable that holds the Jira API token. |

---

### `indexing`

**Optional.** Controls how source files are selected and processed during indexing. All sub-fields are optional; the defaults are designed to work correctly for most TypeScript and JavaScript workspaces without configuration.

#### `indexing.exclude`

**Type:** `string[]`  
**Default:** `["node_modules", "dist", "*.min.js", "*.map", "*.lock", "*.d.ts"]`

Glob patterns for files and directories to exclude from indexing. Patterns are matched against relative file paths within each repo. The default list excludes common build artifacts and package directories.

#### `indexing.language_excludes`

**Type:** list of `{language: string, patterns: string[]}`  
**Default:** `[]`

Per-language exclusion rules. Each entry applies `patterns` only when the file's detected language matches `language`. Useful for excluding generated files in specific languages without excluding them globally.

Example:

```yaml
language_excludes:
  - language: typescript
    patterns:
      - "*.generated.ts"
      - "src/graphql/**"
```

#### `indexing.include_languages`

**Type:** `string[]`  
**Default:** `[]` (all languages indexed)

When non-empty, only files whose detected language is in this list are indexed. Languages are lowercase strings such as `typescript`, `javascript`, `python`, `rust`. An empty list means all supported languages are eligible.

#### `indexing.include_dotfiles`

**Type:** `bool`  
**Default:** `false`

When `true`, files and directories beginning with a dot (e.g., `.config/`) are included in indexing. When `false` (the default), dotfiles and dot-directories are skipped.

#### `indexing.min_file_size`

**Type:** `string` (human-readable size)  
**Default:** `null` (no minimum)

Files smaller than this size are excluded from indexing. Accepts human-readable strings such as `100B`, `1KB`, `1MB`. When omitted, no minimum size is applied.

#### `indexing.max_file_size`

**Type:** `string` (human-readable size)  
**Default:** `"1MB"`

Files larger than this size are excluded from indexing. Accepts human-readable strings such as `512KB`, `2MB`. Prevents very large generated or binary files from degrading parse performance.

#### `indexing.workspace_concurrency`

**Type:** `usize` (optional)  
**Default:** `null` (determined by the indexer)

Controls the number of repos indexed concurrently at the workspace level. When omitted, the indexer chooses a default based on available resources. Setting this to `1` forces sequential per-repo indexing, which is useful for debugging.

### `deployment`

**Optional.** Controls deployment-topology artifact discovery. Deployment indexing runs as part of `index` and `watch` and emits graph nodes for deployments, env var names, workflow jobs, brokers, databases, secrets, and config maps. Secret and env file values are not stored.

| Field | Type | Default | Description |
|---|---|---|---|
| `include` | `string[]` | built-in artifact patterns | Additional deployment artifact globs to scan inside each repo. Use this for non-standard manifest paths. |
| `gitops_roots` | `string[]` | `[]` | Repo-relative directories that contain GitOps or platform deployment manifests. |
| `env_files` | `string[]` | `[]` | Repo-relative env files to scan for variable names. Values are redacted and not persisted. |

The built-in scanner recognizes common Dockerfiles, Docker Compose files, Kubernetes manifests, Kustomize files, explicit Helm chart artifacts, and GitHub Actions workflow YAML. Configured paths must stay inside the repo; paths that escape with `..` are rejected.

## Validation rules

The config parser uses `#[serde(deny_unknown_fields)]` on every struct. Unknown YAML keys are hard errors, not warnings.

| Rule | Detail |
|---|---|
| `repos` must not be empty | At least one repo entry is required. |
| Repo names must be non-empty | Blank names are rejected. |
| Repo names must not contain path separators | `/`, `\`, newlines, carriage returns, and the segments `.` and `..` are rejected. |
| Repo names must not contain null bytes | NUL bytes in names or paths are rejected. |
| Repo paths must be relative | Absolute paths are rejected. |
| Repo paths must not escape the config root | Paths containing `..` components are rejected. |
| Repo names must be unique | Duplicate names within the config are rejected. |
| Repo paths must not overlap | A repo path that is a prefix of another repo's path is rejected. |
| `allow_listed_repos` must reference known repos | Any name not present in `repos` is rejected. |
| `allow_listed_repos` names must be unique | Duplicate entries are rejected. |
| Deployment paths must stay inside each repo | `deployment.include`, `deployment.gitops_roots`, and `deployment.env_files` reject absolute paths and `..` escapes. |
| Repo root symlinks are rejected at index time | A repo whose path resolves through a symlink is rejected during `index` or `watch`. |

## Depth values

The `depth` field on each repo entry (and the `--depth` CLI flag) controls what the indexer extracts from the repo.

| Value | Description |
|---|---|
| `level1` | Lightest pass. File nodes and top-level module structure only. Suitable for large dependency repos where symbol-level detail is not needed. |
| `level2` | File nodes plus top-level declarations (functions, classes, types). No caller/callee edges. |
| `level3` | Full declarations plus call edges within files. No cross-file resolution pass. |
| `full` | Complete indexing including cross-file and cross-repo reference resolution, virtual node construction, payload contract inference, and git history analytics. This is the default and produces the richest graph. |

Depth values are accepted as lowercase strings in YAML. The numeric aliases `1`, `2`, and `3` are accepted for `level1`, `level2`, and `level3` respectively.

## Generated state paths

The files under `.gather-step/` are generated and should not be committed to version control. The `.gather-step/` directory itself is created automatically during `index` and managed by the `clean` command.

For workspace setup instructions, see the [workspace setup guide](/guides/workspace-setup/).
