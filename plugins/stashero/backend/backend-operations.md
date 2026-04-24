# Backend Operations & Arguments

The Python backend for `stashero` acts as a multi-modal plugin command center. Every operation is triggered by passing a specific `mode` string within the arguments payload.

## Global Options
These arguments apply to the application globally or dictate its runtime state and are passed alongside the mode.

* `mode` (string): The operation mode to execute.
* `server_url` (string): The Stash GraphQL URL.
* `cookie_name` (string): Authentication cookie name.
* `cookie_value` (string): Authentication cookie value.
* `using_log` (boolean): Whether to print logs to Stash (default: `True`).
* `dry_run` (boolean): If `True`, simulates operations without saving files (default: `False`).
* `debug_mode` (boolean): Whether to emit verbose debug logs (default: `True`).
* `operations_db_path` (string): Path to SQLite database for history/templates (default: `rename_operations.db`).

## Supported Modes

### Rename & Simulation
* **`rename:run`**: Executes the main renaming logic. Operates as a simulation if global `dry_run=True`.
* **`rename:preview_dry_run`**: A lightweight variant of `rename:run` that quickly predicts new filenames without recording a batch.
  * **Arguments**:
    * `filename_template` (string, required): The Liquid template for the file name.
    * `path_template` (string, optional): The Liquid template for the directory path.
    * `ids` (list of strings): Specific scene IDs to restrict the operation to.
    * `excluded_scene_ids` (list of strings): Scene IDs to omit from the query results.
    * `criteria` (list of objects): Filter criteria payloads for standard Stash scenes filtering.
    * `find_filter` (object): Standard GraphQL pagination and sorting filter payload.
    * `include_warn_error` (boolean): *Used only during dry run.* If `False`, removes operations that result in warnings, errors, fails, or "no change" from the final response (default: `False`).

### Undo & History
* **`undo:undo`**: Reverts a specific file rename.
  * **Arguments**: `undo_operation_id` (string, required).
* **`undo:list_operations`**: Lists all individual historical rename operations.
* **`undo:list_operation_batches`**: Lists high-level batches (groupings of rename operations).
* **`undo:list_batch_operations`**: Lists all atomic rename operations that belong to a specific batch.
  * **Arguments**: `batch_id` (string, required).
* **`undo:undo_batch_operation`**: Rolls back an entire batch of rename operations.
  * **Arguments**: `batch_id` (string, required).
* **`undo:clear_history`**: Purges all history and batches from the operations database.

### Templates
* **`template:list_templates`**: Returns all saved rename templates.
* **`template:save_template`**: Creates a new saved template.
  * **Arguments**: `template_name` (required), `filename_template` (required), `path_template`, `criteria`.
* **`template:update_template`**: Modifies an existing saved template.
  * **Arguments**: `template_id` (required), `template_name` (required), `filename_template` (required), `path_template`, `criteria`.
* **`template:delete_template`**: Deletes a saved template.
  * **Arguments**: `template_id` (string, required).

### Hooks (Event Triggers)
* **`hook:get_settings`**: Retrieves settings for auto-running templates when Stash events occur.
  * **Arguments**: `hook_type` (string, default: `"Scene.Update.Post"`).
* **`hook:save_settings`**: Updates hook auto-run settings.
  * **Arguments**: `hook_type` (string, default: `"Scene.Update.Post"`), `enabled` (boolean), `template_ids` (list of strings).
* **`hook:run`**: The actual execution endpoint called by Stash when an event is triggered.
  * **Arguments**:
    * `hookContext` (object, required): Contains `type` (hook type) and `id` (the ID of the modified Stash object).
    * `hook_type` (string, fallback if missing from `hookContext`).

### Watchdog (Detached Background Worker)
* **`watchdog:save_config`**: Creates or updates one watchdog config row in SQLite. If worker is currently running, it auto-restarts to apply changes.
  * **Arguments**:
    * `id` (string, optional): Existing config ID to update; if omitted, creates a new row.
    * `path` (string, required): Folder to monitor.
    * `operation` (string, required): GraphQL operation (query/mutation text) executed on matching filesystem events.
    * `enabled` (boolean, optional): Whether this config is active (default: `True`).
    * `options` (object or JSON string, optional): Extra config.
      * Supported keys: `event_types` (`modified|created|deleted|moved`), `recursive`, `debounce_seconds`, `request_timeout_seconds`, `variables` (GraphQL variables object).
* **`watchdog:list_config`**: Lists all persisted watchdog config rows.
* **`watchdog:run`**: Starts the detached watchdog worker using all **enabled** rows from SQLite.
* **`watchdog:stop`**: Stops the detached watchdog worker.
* **`watchdog:status`**: Returns worker status (`running` or `stopped`) and current PID.
* **`watchdog:restart`**: Stops any existing worker and starts it again from enabled rows in SQLite.
* **`watchdog:configure`**: Backward-compatible alias for `watchdog:restart`.
  * **Arguments**:
    * `watchdog_runtime_dir` (string, optional for `run/stop/status/restart`): Directory used to persist worker `config.json`, `status.json`, and `watchdog.log`.

### Misc
* **`system:list_selectors`**: Introspects the GraphQL schema and builds the autocomplete token tree (for the UI code editor).
* **`system:ffmpeg_proxy_enable`**: Creates an OS-specific ffmpeg wrapper script inside plugin runtime (`.ffmpeg_proxy/`), logs invoked ffmpeg arguments to a file in that same directory, and updates Stash `configuration.general.ffmpegPath` to the wrapper path.
* **`system:ffmpeg_proxy_reverse`**: If the wrapper script exists, restores Stash `configuration.general.ffmpegPath` to the original path captured during enable using `configureGeneral`.
