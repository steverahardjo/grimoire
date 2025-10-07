# GRIMOIRE
## SQL-Like DBMS Engine

This is a toy database I wrote in **Rust** to learn both the language and the internal workings of a database engine.  
It follows the classic **frontend/backend architecture**, inspired by the design of the SQLite project.

---

## Features

### Frontend
- SQL execution parser (supports `SELECT`, `FROM`, simple `JOIN`, `WHERE`, etc.)
- SQL input tokenizer/parser
- Query planner (converts the AST into backend execution API calls)

### Backend
- `execution/` — execution engine with basic sequential operators
- `access/` — handles in-memory data loading and indexing
- `buffer/` — buffer pool manager for reading/writing data from/to disk
- `storage/` — abstraction layer for page and disk access
- `concur/` — transaction orchestrator and lock manager


