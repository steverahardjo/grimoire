# GRIMOIRE
## SQL LIKE DBMS ENGINE
---
This is a toy database I written on rust to learn rust and internals of how database engine works.
It follows the classic **frontend/backend architecture** basedon the example given by the sqllite project
---
## Features
### Frontend
- SQL execution parser (select, from, join(simple), where etc)
- SQL input parser (select, fro)
- Query Planner (Turn ast into a backend/execution APIs call)

### Backend
- `execution/` engine with basic operators (sequentials)
- `access/` loading data in memory and enable indexing
- `buffer/` pool manager to load data from disk and write to disk
- `storage/ ` abstraction for access to disk & page
- `concur/` orchestrator in case of trnasaction and lock manager

