# Development Pipeline - Blueprints v1

## Global Project Context (copy this into every stage/role prompt)
Project Name: aw-watcher-pipeline-stage
Overall Goal: Develop a lightweight Python watcher for ActivityWatch that monitors a local `current_task.json` file and automatically logs development pipeline stage/task activity.
Languages Used: Python 3.8+
Key Libraries/Frameworks: aw-client, watchdog, argparse, logging, json
Database / Storage: Local JSON file, ActivityWatch buckets
Frontend / GUI (if any): CLI / ActivityWatch WebUI
Current Architecture Summary: Modular Python package with `main` (CLI/Signals), `watcher` (Watchdog Observer + Debounce), `client` (AW Wrapper + Retries), and `config` (Priority Loading).
Last Major Change: Stage 2 functional correctness verified; core logic operational.

## Refined Role Starters (see role_starters.md for the three clean role prompts)

## Stage 0: Requirements & Planning (expanded for robustness)

Goal: Capture complete requirements, skill gaps, tech stack decisions, feasibility, risks, milestones, and success criteria before any code.

Recommended Expert Role: Architect (primary), then PM for breakdown

Template:
### STAGE 0: Requirements & Planning

Global Project Context:
Project Name: aw-watcher-pipeline-stage
Overall Goal: Add a new official watcher to ActivityWatch that automatically detects and logs which stage/task of my development pipeline I'm currently working on by monitoring a local current_task.json file.
Languages Used: Python (primary), possibly small Rust components later
Key Libraries/Frameworks: watchdog, aw-client, json, pathlib
Database / Storage: None (file-based)
Frontend / GUI (if any): None (background watcher)
Current Architecture Summary: Modular Python package with main (CLI), watcher (Observer), client (AW Wrapper), config.
Last Major Change: Stage 2 functional correctness verified; core logic operational.

What I Want to Build: 
A custom ActivityWatch watcher that watches for changes to current_task.json in my project folder, reads the current_stage and current_task, then sends heartbeats/events to a new bucket (e.g. "aw-watcher-pipeline-stage") so I can see time spent per pipeline stage in the ActivityWatch UI.

Skills I Already Have: 
- Intermediate Python
- Basic Linux command line (Pop!_OS)
- Familiarity with JSON, pathlib, and file watching concepts
- Installed watchdog + aw-client successfully
- Experience using our development pipeline templates

Skills I Need to Learn: 
- How ActivityWatch watchers are structured (especially existing Python watchers like aw-watcher-window, aw-watcher-afk)
- Proper use of aw-client (creating buckets, sending heartbeats, pulsetime)
- GitHub fork + clone + branch + PR workflow
- ActivityWatch testing and development setup
- Basic contribution guidelines and code style

Known Constraints (time, budget, platform, etc.): 
- Time: Part-time hobby project, 5-10 hours per week
- Budget: $0 (all open source tools)
- Platform: Primarily Pop!_OS (Linux), should eventually support Windows/macOS
- Must remain lightweight and local-only

Non-functional Requirements (performance, security level, offline support, etc.): 
- Very low CPU/memory usage (<1% CPU when idle)
- 100% offline and local (no network calls)
- Privacy-first: no telemetry, all data stays on machine
- Robust error handling (malformed JSON, missing file, permission issues)
- Cross-platform file watching support

Key Features (prioritized): 
1. Monitor current_task.json for modifications
2. Parse current_stage and current_task fields
3. Send categorized heartbeats to ActivityWatch bucket
4. Graceful handling of missing/corrupt JSON
5. Clear logging to terminal
6. (Stretch) Auto-start with ActivityWatch
7. (Stretch) Support for checklist progress tracking

Success Metrics: 
- Watcher runs without crashing for 30+ minutes
- Correct events appear in ActivityWatch UI with proper category (e.g. "Stage 8 - Final Integration Review")
- Can see accurate time spent per stage in reports
- Code is clean enough to submit as a PR to ActivityWatch repo

[Initial Research & Requirements Summary]
- Finalized Requirements (from stage0RFI.md):
  - **File Monitoring**: Watch `current_task.json` (configurable via `--watch-path`, env var, or config file).
  - **Change Detection**: Use `watchdog` events. Debounce interval: 1.0s. Compare content hash to avoid duplicate processing.
  - **Data Parsing**: Extract `current_stage`, `current_task`, `status`, `project_id`, `start_time`, `metadata`.
  - **Heartbeats**:
    - Send immediately on state change.
    - Send periodically every 30s while active (`status="in_progress"`).
    - Use `pulsetime=120` for server-side merging.
    - Bucket: `aw-watcher-pipeline-stage_{hostname}`, Event Type: `current-pipeline-stage`.
  - **Robustness**: Handle malformed JSON, file absence, and offline mode (`queued=True`).
  - **Logging**: Console logging (INFO/WARNING/ERROR) + optional `--log-file`.


Architect Version → copy to Architect LLM
You are a Senior Software Architect LLM. Your job is to produce high-level plans, ask clarifying questions, and issue directives to the Senior Project Manager.
Current Project Context:
Project Name: aw-watcher-pipeline-stage
Overall Goal: Add a new official watcher to ActivityWatch that automatically detects and logs which stage/task of my development pipeline I'm currently working on by monitoring a local current_task.json file.
Languages Used: Python (primary), possibly small Rust components later
Key Libraries/Frameworks: watchdog, aw-client, json, pathlib
Database / Storage: None (file-based)
Frontend / GUI (if any): None (background watcher)
Current Architecture Summary: New standalone watcher that integrates with ActivityWatch's existing event/heartbeat system
Last Major Change: Initial Stage 0 planning
Stage 0: Requirements & Planning
What I Want to Build: A custom ActivityWatch watcher that watches for changes to current_task.json in my project folder, reads the current_stage and current_task, then sends heartbeats/events to a new bucket.
Skills I Already Have: Intermediate Python, Linux CLI, JSON handling, watchdog + aw-client installed.
Skills I Need to Learn: ActivityWatch watcher architecture, aw-client best practices, GitHub contribution workflow.
Known Constraints: Part-time hobby (5-10 hrs/week), $0 budget, Linux primary target.
Non-functional Requirements: Low CPU/memory, fully offline/local, robust error handling, privacy-first.
Key Features (prioritized): 1. Monitor JSON file 2. Parse stage/task 3. Send heartbeats 4. Error handling 5. Terminal logging
Success Metrics: Stable watcher, correct events in UI, time tracked per stage.
Before providing any code or architecture designs, please ask me at least 5-7 detailed technical questions to clarify my requirements.
If you have any other questions, please ask! Otherwise, please respond in this exact format:
DIRECTIVE FORMAT:
Prompt to Senior Project Manager:
Stage 0 - Requirements & Planning
[Your detailed directive(s) / architecture guidance here ]

PM Version → copy to SeniorPM LLM (after Architect responds)
DIRECTIVE FORMAT:
Prompt to Senior Project Manager:
Stage 0 - Requirements & Planning
To clarify and solidify the requirements before moving to architecture or implementation, obtain detailed answers from the stakeholder to at least these 7 technical questions (feel free to add follow-ups as needed):

Provide a concrete example of the current_task.json file contents, including exact field names (e.g., "current_stage", "current_task"), data types (strings, enums, numbers, timestamps?), any nested objects, and optional fields like project identifier, start time, or metadata.
What is the expected filesystem location of current_task.json (absolute/relative path)? Should the watcher support a configurable path via CLI arguments (e.g., --file-path), environment variables, or a config file, or assume a hardcoded default?
Define precisely what constitutes a triggering "change": any modification to the file (size/timestamp), or only meaningful changes to specific fields like current_stage or current_task? Should we debounce rapid successive writes?
What heartbeat/pulse interval is desired while a stage/task is active (e.g., every 15s, 30s, 60s)? Should we send an immediate event on file change, followed by periodic heartbeats until the next change?
What exact fields and structure should be included in the ActivityWatch heartbeat/event payload sent to aw-client (beyond stage/task)? E.g., include original JSON timestamp, file path, computed duration, project name, or custom metadata?
What bucket name and event type should be used (e.g., bucket="aw-watcher-pipeline-stage", event type="pipeline-stage")? Any preferences for how events appear in the ActivityWatch UI (category, color coding, etc.)?
Detail desired error handling and robustness: initial file absence, invalid/malformed JSON, read permission errors, watchdog failures, aw-client connection issues (even if offline), or file deletion. Should it retry, log warnings/errors to console/file, exit gracefully, or continue silently after recovery?

Once answers are provided, compile a finalized requirements document and share it with me for review before advancing to high-level architecture and component breakdown. Prioritize low-CPU watchdog usage and offline-only operation per non-functional requirements.

Engineer Version → copy to SeniorSE LLM (after PM responds)
[PASTE ENGINEER ROLE STARTER HERE]




## Global Project Context (copy this block into every stage/role prompt)
Project Name: 
Overall Goal: 
Languages Used: 
Key Libraries/Frameworks: 
Database / Storage: 
Frontend / GUI (if any): 
Current Architecture Summary: 
Last Major Change: 

## Quick Iteration Mode (save this separately as quick_iteration_mode.md)
When working rapidly on the same file/module in one session, use this shortened version instead of full context:

### QUICK ITERATION - Stage #[STAGE_NUMBER]
Current File/Module: 
Specific Focus: 
Code Snippet: 
[PASTE CODE]

Architect / PM / Engineer role starters still apply, but omit full Global Project Context.

## Stage 1: Code Structure & Style

Goal: Ensure code follows language conventions, is readable, modular, and maintainable. Catch style violations early.

Recommended Expert Role: Senior Systems Engineer (heavy focus on refactoring)

Template:
### STAGE 1: Code Structure & Style

Global Project Context:
Project Name: aw-watcher-pipeline-stage
Overall Goal: Develop a lightweight Python watcher for ActivityWatch that monitors a local `current_task.json` file and automatically logs development pipeline stage/task activity.
Languages Used: Python 3.8+
Key Libraries/Frameworks: aw-client, watchdog, argparse, logging, json
Database / Storage: Local JSON file, ActivityWatch buckets
Frontend / GUI (if any): CLI / ActivityWatch WebUI
Current Architecture Summary: Modular Python package with `main` (CLI/Signals), `watcher` (Watchdog Observer + Debounce), `client` (AW Wrapper + Retries), and `config` (Priority Loading).
Last Major Change: Stage 2 functional correctness verified; core logic operational.

Code to Review (paste full module/file here):
All files in `aw_watcher_pipeline_stage/`: `main.py`, `watcher.py`, `client.py`, `config.py`.

Specific Style/Readability Concerns (optional):
- Ensure PEP 8 compliance (black/isort).
- Verify type hints (mypy strictness) and Google-style docstrings.
- Check modular separation: Config loading vs. Watcher logic vs. Client communication.
- Enforce low-CPU design: Event-driven observer only (no polling loops).
- Thread safety: Verify `threading.Lock` usage in `PipelineEventHandler`.
- Minimal dependencies: `watchdog`, `aw-client` only.

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 1 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 1 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 1 details]

Checklist:
- Updated Global Project Context?
- Saved refactored code to file?
- Updated Last Major Change?
- Noted any new directives or decisions?

## Stage 2: Functional Correctness

Goal: Verify the code matches intended requirements and behaves correctly under normal conditions.

Recommended Expert Role: Architect + Senior Systems Engineer

Template:
### STAGE 2: Functional Correctness

Global Project Context:
Project Name: aw-watcher-pipeline-stage
Overall Goal: Develop a lightweight Python watcher for ActivityWatch that monitors a local `current_task.json` file and automatically logs development pipeline stage/task activity.
Languages Used: Python 3.8+
Key Libraries/Frameworks: aw-client, watchdog, argparse, logging, json
Database / Storage: Local JSON file, ActivityWatch buckets
Frontend / GUI (if any): CLI / ActivityWatch WebUI
Current Architecture Summary: Modular Python package with `main` (CLI/Signals), `watcher` (Watchdog Observer + Debounce), `client` (AW Wrapper + Retries), and `config` (Priority Loading).
Last Major Change: Stage 2 functional correctness verified; core logic operational.

Intended Behavior / Requirements Summary:
- Watcher detects modifications to `current_task.json` and parses JSON content.
- Debounces rapid file events (1.0s interval) to prevent duplicate processing.
- Sends heartbeats to ActivityWatch server with `current-pipeline-stage` event type (pulsetime=120s).
- Handles connection failures (queued mode) and malformed JSON without crashing.
- Handles file deletion/movement by pausing heartbeats.
- Sends periodic heartbeats every 30s for active tasks.

Key Functions/Modules to Verify:
- `PipelineEventHandler._parse_file`: JSON parsing, hash comparison, and state change detection.
- `PipelineEventHandler._parse_file_wrapper`: Debounce logic via threading.Timer.
- `PipelineEventHandler.check_periodic_heartbeat`: Periodic heartbeat logic.
- `PipelineEventHandler.on_deleted` / `on_moved`: Handling file deletion/moves and pause state.
- `PipelineClient.send_heartbeat`: Retry logic, metadata flattening, and aw-client interaction.
- `main`: Signal handling and main loop heartbeat interval.
- `config.load_config`: Configuration priority (CLI > Env > File > Defaults).

Code to Analyze:
`aw_watcher_pipeline_stage/watcher.py`, `aw_watcher_pipeline_stage/client.py`, `aw_watcher_pipeline_stage/main.py`, `aw_watcher_pipeline_stage/config.py`

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 2 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 2 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 2 details]

Checklist:
- Updated Global Project Context?
- Saved any corrected code?
- Updated Last Major Change?
- Noted any requirement gaps found?

## Stage 3: Debugging / Specific Issues

Goal: Diagnose and fix targeted bugs or unexpected behavior.

Recommended Expert Role: Senior Systems Engineer

Template:
### STAGE 3: Debugging / Specific Issues

Global Project Context:
Project Name: aw-watcher-pipeline-stage
Overall Goal: Develop a lightweight Python watcher for ActivityWatch that monitors a local `current_task.json` file and automatically logs development pipeline stage/task activity.
Languages Used: Python 3.8+
Key Libraries/Frameworks: aw-client, watchdog, argparse, logging, json
Database / Storage: Local JSON file, ActivityWatch buckets
Frontend / GUI (if any): CLI / ActivityWatch WebUI
Current Architecture Summary: Modular Python package with `main` (CLI/Signals), `watcher` (Watchdog Observer + Debounce), `client` (AW Wrapper + Retries), and `config` (Priority Loading).
Last Major Change: Stage 2 functional correctness verified; core logic operational.

Specific Issue:
Error Message: 
File/Line: 
Reproduction Steps: 
Expected Behavior: 
Actual Behavior: 

Code Snippet:

What I've Already Tried:

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 3 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 3 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 3 details]

Checklist:
- Updated Global Project Context?
- Saved fixed code?
- Updated Last Major Change?
- Reproduced fix locally?

## Stage 4: Security Audit

Goal: Identify vulnerabilities, data handling issues, and security best practices gaps.

Recommended Expert Role: Architect (threat modeling) + Senior Systems Engineer

Template:
### STAGE 4: Security Audit

Global Project Context:
[PASTE ABOVE]

Specific Security Focus Areas (e.g., input validation, auth, data storage):

Code / Modules Handling Sensitive Data:

Threat Model Notes:

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 4 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 4 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 4 details]

Checklist:
- Updated Global Project Context?
- Saved security fixes?
- Updated Last Major Change?
- Reviewed dependencies/vulnerabilities?

## Stage 5: Testing Strategy

Goal: Define and implement comprehensive tests (unit, integration, edge cases).

Recommended Expert Role: Senior Systems Engineer

Template:
### STAGE 5: Testing Strategy

Global Project Context:
[PASTE ABOVE]

Key Functions/Modules to Test:
Test Types Needed (unit, integration, property-based, etc.):

Existing Tests (if any):

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 5 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 5 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 5 details]

Checklist:
- Updated Global Project Context?
- Saved test files/code?
- Updated Last Major Change?
- Ran tests successfully?

## Stage 6: Performance / Optimization

Goal: Identify bottlenecks, improve efficiency, and optimize resource usage.

Recommended Expert Role: Senior Systems Engineer

Template:
### STAGE 6: Performance / Optimization

Global Project Context:
[PASTE ABOVE]

Known Performance Concerns or Hot Paths:
Benchmarks / Profiling Data (if available):

Code to Optimize:

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 6 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 6 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 6 details]

Checklist:
- Updated Global Project Context?
- Saved optimized code?
- Updated Last Major Change?
- Verified performance improvement?

## Stage 7: Documentation & Maintainability

Goal: Add comments, docstrings, README updates, type hints, and architectural notes.

Recommended Expert Role: Senior Systems Engineer + Architect

Template:
### STAGE 7: Documentation & Maintainability

Global Project Context:
[PASTE ABOVE]

Files/Modules Needing Documentation:
Specific Documentation Needs:

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 7 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 7 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 7 details]

Checklist:
- Updated Global Project Context?
- Saved documentation updates?
- Updated Last Major Change?
- README current?

## Stage 8: Final Integration Review

Goal: Ensure the module integrates well with the overall architecture, check consistency, and prepare for next iteration.

Recommended Expert Role: Architect + Senior Project Manager

Template:
### STAGE 8: Final Integration Review

Global Project Context:
[PASTE ABOVE]

Modules Affected by Recent Changes:
Integration Points / Dependencies:

Overall Concerns:

Architect Version → copy to Architect LLM:
[PASTE ARCHITECT ROLE STARTER + fill Stage 8 details]

Senior Project Manager Version → copy to SeniorPM LLM:
[PASTE PM ROLE STARTER + fill Stage 8 details]

Senior Systems Engineer Version → copy to SeniorSE LLM:
[PASTE ENGINEER ROLE STARTER + fill Stage 8 details]

Checklist:
- Updated Global Project Context?
- Merged/integrated changes?
- Updated Last Major Change?
- Ready for next stage or release?
