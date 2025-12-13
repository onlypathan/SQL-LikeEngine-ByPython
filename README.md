# SQL-Like Query Engine from Scratch

## Project for DSCI 551 – Fall 2025  
**Author**: Rizwan Ahasan Pathan  
**Streamlit App**: [https://onlypathan.streamlit.app](https://onlypathan.streamlit.app)

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
