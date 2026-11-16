---
description: Put the demo back to its starting state
---
Confirm with the user before doing anything, because this replaces generated data.
Then: regenerate the landing zone (`.\tasks.ps1 data -Force`), restore any seed or model files
that were edited during the demo (`git status`, then `git restore` only the files the user confirms),
and run `.\tasks.ps1 reset -Env dev -Yes`. That takes the act 2 seed fixes out (`demo-fix -Fix undo`),
regenerates the landing zone itself (so the `data -Force` step is optional), reloads bronze, empties the
dbt schemas, republishes the dbt job and rebuilds; about 12 minutes. Run sheet: `docs/DEMO_SCRIPT.md`.
Never run against prod.
