---
description: Put the demo back to its starting state
---
Confirm with the user before doing anything, because this replaces generated data.
Then: regenerate the landing zone (`.\tasks.ps1 data -Force`), restore any seed or model files
that were edited during the demo (`git status`, then `git restore` only the files the user confirms),
and run `.\tasks.ps1 reset -Env dev -Yes`. That regenerates the landing zone itself (so the
`data -Force` step is optional), reloads bronze, empties the dbt schemas and rebuilds; about 10 minutes.
Never run against prod.
