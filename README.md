---
author: Matt Humphrey
date_started: 2025-07-07
date_finished: 2025-07-08
tags:
- harmonisation
---

## Aim

Separate the Gen1 and Gen2 data from the physical assessment dataset for Y5.

The relevant variables to extract and rename are specified in `src/config/variables.py`.
The script to perform this task is in `src/main.py`.

## TODO

- [x] Extract parent variables from PA, and move to separate dataset
- [x] Rename those variables to align/harmonise
- [x] Use child ID and BPCD to determine the corresponding parent ID