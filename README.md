
# SQL-Like DataFrame Query Engine from Scratch (Python)

**Author:** Rizwan Ahasan Pathan  
**Affiliation:** M.S. in Applied Data Science, University of Southern California (USC)  
**Project Type:** Systems Programming · Data Engineering  
**Focus Areas:** Query Engines, Data Structures, Algorithm Design, Backend Systems  
**Streamlit App**: [https://onlypathan.streamlit.app](https://onlypathan.streamlit.app)

---

## Overview

This project implements a **fully functional SQL-like query engine from scratch in Python**, designed to demonstrate how relational data systems operate internally.  
Rather than relying on high-level libraries such as **pandas**, **csv**, or **json**, all core data-processing logic is built using **native Python data structures**.

The system supports essential SQL operations—**SELECT, WHERE, JOIN, GROUP BY, ORDER BY, and aggregation**—and includes a **Streamlit-based interactive interface** that allows users to construct and execute queries visually or programmatically.

The result is a lightweight, in-memory analytical engine that closely mirrors real-world database behavior while remaining transparent and extensible.

---

## Project Objectives

- Understand the internal mechanics of SQL query execution  
- Implement relational operations using native Python data structures  
- Design a modular and extensible query-processing architecture  
- Support indexing and primary keys for performance optimization  
- Provide an interactive web interface for query execution  
- Enable real-world data exploration without SQL syntax

---

## Files Included

- `csv_parser.py` – Custom parser to read CSV files (no pandas/csv used)
- `data_loader.py` – Loads tables and sets primary keys/indexes
- `my_custom_db.py` – Core class that supports SQL-like operations
- `index.py` – Runs queries via `select_query()` function
- `Final_Project_Report_By_Rizwan_Ahasan_Pathan.pdf` – Final Report (8 pages)
- `restaurant_info.csv`, `inspection_info.csv`, etc. – Dataset files *(if submitting)*

---

##  How to Run (Locally)

### Option 1: Run the Backend Query Engine
```bash
python index.py
```

### Option 2: Launch the Web App
Make sure you have Streamlit installed:
```bash
pip install streamlit
streamlit run index.py
```

---

## Features Supported
- SELECT / Projection
- WHERE (Filtering)
- GROUP BY & Aggregation (AVG, MIN, MAX, COUNT)
- JOINs (Inner, Left)
- ORDER BY (with direction)
- Primary Key & Indexing

---

##  Future Work
- User file uploads
- Chunked CSV loading for large datasets
- Query saving & export
- Chart-based visual summaries

---

##  References
- [Streamlit Docs](https://docs.streamlit.io)
- [Python Docs](https://docs.python.org/3/)
- [SQLite Query Planner](https://sqlite.org/queryplanner.html)
- [PostgreSQL Indexes](https://www.postgresql.org/docs/current/indexes.html)
- [Project GitHub Repo (if applicable)](https://github.com/onlypathan/SQL-LikeEngine-ByPython)
