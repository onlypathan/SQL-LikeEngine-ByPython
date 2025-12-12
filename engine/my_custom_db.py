from csv_parser import CSVParser
from data_loader import DataLoader


class MyCustomMemoryDB:
    def __init__(self):
        # In-memory database structure:
        # { table_name: { rows, indexes, primary_key, ... } }
        self.database = {}

        # Console color codes for error highlighting
        self.MessageBGcolourS = "\033[48;2;253;226;224m\033[30m"
        self.MessageBGcolourE = "\033[0m"

    def create_table(self, name, primary_key="id", indexes=None, foreign_keys=None):
        # Create a new table definition
        self.database[name] = {
            "rows": {},                 # row storage (pk -> row)
            "next_id": 1,               # auto-increment counter
            "primary_key": primary_key,
            "indexes": {col: {} for col in (indexes or [])},
            "foreign_keys": foreign_keys or {}
        }

    def insert(self, table_name, row):
        # Insert a single row into a table
        table = self.database[table_name]
        pk = table["primary_key"]

        # Auto-assign primary key if missing
        if pk not in row:
            row[pk] = table["next_id"]
            table["next_id"] += 1

        # Enforce foreign key constraints if defined
        for fk_col, (ref_table, ref_col) in table["foreign_keys"].items():
            if row[fk_col] not in self.database[ref_table]["indexes"].get(ref_col, {}):
                raise ValueError(
                    f"Foreign key constraint failed: {fk_col}={row[fk_col]} "
                    f"not found in {ref_table}.{ref_col}"
                )

        # Store row by primary key
        key = row[pk]
        table["rows"][key] = row

        # Update secondary indexes
        for index_col in table["indexes"]:
            val = row.get(index_col)
            if val is not None:
                table["indexes"][index_col].setdefault(val, []).append(key)

    def get_all(self, table_name):
        # Return all rows from a table
        return list(self.database[table_name]["rows"].values())

    def inner_join(self, left_table, right_table, left_key, right_key):
        # Simple nested-loop INNER JOIN
        left_rows = self.get_all(left_table)
        right_rows = self.get_all(right_table)
        joined = []

        for l in left_rows:
            for r in right_rows:
                if l.get(left_key) == r.get(right_key):
                    joined.append(
                        {f"{left_table}.{k}": v for k, v in l.items()} |
                        {f"{right_table}.{k}": v for k, v in r.items()}
                    )
        return joined

    def left_join(self, left_table, right_table, left_key, right_key):
        # LEFT JOIN implementation
        left_rows = self.get_all(left_table)
        right_rows = self.get_all(right_table)
        joined = []

        for l in left_rows:
            matched = False
            for r in right_rows:
                if l.get(left_key) == r.get(right_key):
                    joined.append(
                        {f"{left_table}.{k}": v for k, v in l.items()} |
                        {f"{right_table}.{k}": v for k, v in r.items()}
                    )
                    matched = True
            if not matched:
                joined.append(
                    {f"{left_table}.{k}": v for k, v in l.items()} |
                    {f"{right_table}.{k}": None for k in right_rows[0].keys()}
                )
        return joined

    def select_where(self, table_name, where):
        """
        WHERE filtering with:
        - AND / OR logic
        - index / primary key optimization
        - support for prefixed column names
        """
        table = self.database[table_name]
        pk = table["primary_key"]
        indexes = table.get("indexes", {})
        rows_by_pk = table["rows"]

        if not where:
            return list(rows_by_pk.values())

        def get_value(row, col):
            # Resolve column value from prefixed or unprefixed keys
            if col in row:
                return row[col]
            short = col.split(".", 1)[1] if "." in col else col
            for k, v in row.items():
                if k.endswith("." + short) or k == short:
                    return v
            return None

        def match_row(row, col, op, val):
            # Evaluate a single condition
            r = get_value(row, col)

            if isinstance(r, str) and isinstance(val, str):
                r, val = r.lower(), val.lower()

            try:
                if op == "=":  return r == val
                if op == "!=": return r != val
                if op == ">":  return r is not None and r > val
                if op == "<":  return r is not None and r < val
                if op == ">=": return r is not None and r >= val
                if op == "<=": return r is not None and r <= val
                if op == "in": return r in val
                if op == "not in": return r not in val
            except Exception:
                return False
            return False

        # Try to narrow candidate rows using PK or index
        candidate_keys = None
        first = where[0][0]

        if first[1] == "=":
            col = first[0].split(".")[-1]
            if col == pk:
                candidate_keys = [first[2]]
            elif col in indexes:
                candidate_keys = indexes[col].get(first[2], [])

        rows = (
            [rows_by_pk[k] for k in candidate_keys if k in rows_by_pk]
            if candidate_keys is not None else
            list(rows_by_pk.values())
        )

        result = []
        for row in rows:
            if all(match_row(row, *cond) for group in where for cond in group if isinstance(cond, tuple)):
                result.append(row)

        return result

    def group_by(self, rows, group_key, agg_col, agg_fn):
        # GROUP BY with aggregation
        from collections import defaultdict

        funcs = {
            "avg": lambda v: round(sum(v)/len(v), 2) if v else None,
            "sum": sum,
            "count": len,
            "max": max,
            "min": min
        }

        if agg_fn not in funcs:
            return []

        grouped = defaultdict(list)
        for r in rows:
            grouped[r.get(group_key)].append(r.get(agg_col))

        table, col = agg_col.split(".", 1)
        return [
            {
                group_key: k,
                f"{table}.{agg_fn}_{col}": funcs[agg_fn]([v for v in vals if isinstance(v, (int, float))])
            }
            for k, vals in grouped.items()
        ]

    def project_columns(self, rows, select):
        # SELECT specific columns
        result = []
        for row in rows:
            new_row = {}
            for col in select:
                if col in row:
                    new_row[col] = row[col]
                else:
                    short = col.split(".")[-1]
                    new_row[col] = next((v for k, v in row.items() if k.endswith(short)), None)
            result.append(new_row)
        return result

    def order_by_rows(self, rows, order_by, descending=False):
        # ORDER BY sorting
        if isinstance(order_by, str):
            order_by = [order_by]
        if isinstance(descending, bool):
            descending = [descending] * len(order_by)

        for col, desc in reversed(list(zip(order_by, descending))):
            rows.sort(key=lambda r: (r.get(col) is None, r.get(col)), reverse=desc)
        return rows

    def reorder_conditions(self, conditions):
        # Normalize WHERE conditions order
        result = []
        for group in conditions:
            conds, ops = [], []
            for item in group:
                if isinstance(item, str):
                    ops.append(item.upper())
                else:
                    conds.append(item)
            result.append(conds + ops)
        return result

    def _check_validate(self, from_table, joins=None, where=None,
                        columns=None, agg_fn=None, agg_col=None, order_by=None):
        # Validate table and column references before execution
        tables = [from_table] + [j[0] for j in joins or []]
        for t in tables:
            if t not in self.database:
                print(f"{self.MessageBGcolourS}Table not found: {t}{self.MessageBGcolourE}")
                return False
        return True

    def select_query(self, from_table, joins=None, where=None,
                     group_by=None, agg_col=None, agg_fn=None,
                     columns=None, order_by=None, descending=False):
        # Main query execution pipeline

        if where:
            where = self.reorder_conditions(where)

        if not self._check_validate(from_table, joins, where, columns, agg_fn, agg_col, order_by):
            return []

        # Load base table
        rows = [
            {f"{from_table}.{k}": v for k, v in r.items()}
            for r in self.database[from_table]["rows"].values()
        ]
        base = from_table

        # Apply joins
        if joins:
            for join_table, (lk, rk), jt in joins:
                rows = (
                    self.inner_join(base, join_table, lk, rk)
                    if jt == "inner" else
                    self.left_join(base, join_table, lk, rk)
                )
                base = "tmpTable"
                self.create_table(base)
                for i, r in enumerate(rows):
                    self.insert(base, {"id": i + 1, **r})

        # Apply WHERE
        if where:
            rows = self.select_where(base, where)

        # Apply GROUP BY
        if agg_fn and agg_col:
            rows = self.group_by(rows, group_by, agg_col, agg_fn)

        # Apply SELECT
        if columns:
            rows = self.project_columns(rows, columns)

        # Apply ORDER BY
        if order_by:
            rows = self.order_by_rows(rows, order_by, descending)

        # Cleanup temp table
        self.database.pop("tmpTable", None)

        return rows


def MyCustomMiniSQLEngine():
    # Factory method to build and populate the database
    parser = CSVParser()
    db = MyCustomMemoryDB()
    loader = DataLoader(db, parser)
    loader.load_all()
    return db
