---
description: Put the demo back to its starting state
---
Confirm with the user before doing anything, because this replaces generated data.
Then: regenerate the landing zone (`.\tasks.ps1 data -Force`), restore any seed or model files
that were edited during the demo (`git status`, then `git restore` only the files the user confirms),
and, once `.\tasks.ps1 reset` exists (Phase 5), run it for the dev environment. Never run against prod.
