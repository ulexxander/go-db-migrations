# Go DB migrations

Modular migrations system supporting in theory multiple databases and migration sources.

Born on a private project where I decided to replace already existing implementation with more flexible one that was developed in TDD style.
But eventually it seemed to be overkill for that particular project so I moved it here.

Currently it needs to be slightly refactored (because living in previous project had some constraints) and then it will be documented.
For now you can refer to test files.

## Supported databases

- PostreSQL

## Supported migration sources

- SourceDir (read from directory)
- SourceDirect (read from Go slice, used mostly in tests, but can be useful anyway)
