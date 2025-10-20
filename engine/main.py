# --- csv_parser.py ---
class CSVParser:
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def parse(self, filepath):
        import csv
        def infer(value):
            value = value.strip()
            if not value:
                return None
            for cast in (int, float):
                try:
                    return cast(value)
                except ValueError:
                    continue
            return value

        with open(filepath, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            headers = [h.strip() for h in next(reader)]
            for row_values in reader:
                values = [infer(val) for val in row_values]
                yield dict(zip(headers, values))


class DataLoader:
    def __init__(self, db, parser):
        self.db = db
        self.parser = parser

    def create_tables(self):
        self.db.create_table("zip_code", primary_key="Zip_Code_ID", indexes=["Zip_Code"])
        self.db.create_table("demographics_info", primary_key="Demographics_Info_ID", indexes=["F_Zip_Code_ID"])
        self.db.create_table("inspection_info", primary_key="Inspection_Info_ID", indexes=["F_Restaurant_Info_ID"])
        self.db.create_table("restaurant_info", primary_key="Restaurant_Info_ID", indexes=["F_Zip_Code_ID", "Restaurant_Name", "Categories"])

    def load_all(self):
        self.create_tables()
        self.load_csv("../data/zip_code.csv", "zip_code")
        self.load_csv("../data/demographics_info.csv", "demographics_info")
        self.load_csv("../data/inspection_info.csv", "inspection_info")
        self.load_csv("../data/restaurant_info.csv", "restaurant_info")

    def load_csv(self, filepath, table_name):
        for row in self.parser.parse(filepath):
            self.db.insert(table_name, row)




class MyCustomMemoryDB:
    def __init__(self):
        self.database = {}
        self.MessageBGcolourS = "\033[48;2;253;226;224m\033[30m"
        self.MessageBGcolourE = "\033[0m"

    def create_table(self, name, primary_key="id", indexes=None, foreign_keys=None):
        self.database[name] = {
            "rows": {},
            "next_id": 1,
            "primary_key": primary_key,
            "indexes": {col: {} for col in (indexes or [])},
            "foreign_keys": foreign_keys or {}
        }

    def insert(self, table_name, row):
        table = self.database[table_name]
        pk = table["primary_key"]

        if pk not in row:
            row[pk] = table["next_id"]
            table["next_id"] += 1

        for fk_col, (ref_table, ref_col) in table["foreign_keys"].items():
            if row[fk_col] not in self.database[ref_table]["indexes"].get(ref_col, {}):
                raise ValueError(f"Foreign key constraint failed: {fk_col}={row[fk_col]} not found in {ref_table}.{ref_col}")

        key = row[pk]
        table["rows"][key] = row

        for index_col in table["indexes"]:
            val = row.get(index_col)
            if val is not None:
                if val not in table["indexes"][index_col]:
                    table["indexes"][index_col][val] = []
                table["indexes"][index_col][val].append(key)

    def get_all(self, table_name):
        return list(self.database[table_name]["rows"].values())

    def print_table(self, table_name):
        print(self.database[table_name])
        return

    def print_result_table(self, rows):
        if not rows:
            print("No data found.")
            return

        def clean(col): return col.split('.', 1)[-1]

        raw_cols = list(rows[0].keys())
        disp_cols = [clean(c) for c in raw_cols]

        col_widths = {
            disp_col: max(len(disp_col), max(len(str(row.get(raw_col, ""))) for row in rows))
            for raw_col, disp_col in zip(raw_cols, disp_cols)
        }

        header = " | ".join(f"{disp_col:<{col_widths[disp_col]}}" for disp_col in disp_cols)
        separator = "-+-".join("-" * col_widths[disp_col] for disp_col in disp_cols)
        print(header)
        print(separator)

        for row in rows:
            line = " | ".join(
                f"{str(row.get(raw_col, '')):<{col_widths[clean(raw_col)]}}"
                for raw_col in raw_cols
            )
            print(line)

    def inner_join(self, left_table, right_table, left_key, right_key):
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
        left_rows = self.get_all(left_table)
        right_rows = self.get_all(right_table)
        joined = []
        for l in left_rows:
            match_found = False
            for r in right_rows:
                if l.get(left_key) == r.get(right_key):
                    joined.append(
                        {f"{left_table}.{k}": v for k, v in l.items()} |
                        {f"{right_table}.{k}": v for k, v in r.items()}
                    )
                    match_found = True
            if not match_found:
                joined.append(
                    {f"{left_table}.{k}": v for k, v in l.items()} |
                    {f"{right_table}.{k}": None for k in right_rows[0].keys()}
                )
        return joined

 
    def select_where(self, table_name, where):
        """
        Unified version of select_where():
          ✅ Uses primary key or index if available
          ✅ Supports AND / OR grouped logic
          ✅ Handles prefixed column names (table.col)
          ✅ Works for joined temp tables
          ✅ Case-insensitive for strings
        """
    
        table = self.database[table_name]
        pk = table["primary_key"]
        indexes = table.get("indexes", {})
        rows_by_pk = table["rows"]
    
        # If no WHERE clause → return all rows
        if not where:
            return list(rows_by_pk.values())
    
        # ------------------------------------------------------------
        # Helper: get value from possibly-prefixed key
        # ------------------------------------------------------------
        def get_value(row, col):
            if col in row:
                return row[col]
            short = col.split(".", 1)[1] if "." in col else col
            if short in row:
                return row[short]
            for k, v in row.items():
                if k.endswith("." + short):
                    return v
            return None
    
        # ------------------------------------------------------------
        # Helper: evaluate comparison between one row & condition
        # ------------------------------------------------------------
        def match_row(row, col, op, val):
            r = get_value(row, col)
            if isinstance(r, str) and isinstance(val, str):
                r, val = r.strip().lower(), val.strip().lower()
            try:
                if op == "=":      return r == val
                if op == "!=":     return r != val
                if op == ">":      return r is not None and r > val
                if op == "<":      return r is not None and r < val
                if op == ">=":     return r is not None and r >= val
                if op == "<=":     return r is not None and r <= val
                if op == "in":     return r in val
                if op == "not in": return r not in val
            except Exception:
                return False
            return False
    
        # ------------------------------------------------------------
        # Step 1: Try index / primary-key optimization for first condition
        # ------------------------------------------------------------
        candidate_keys = None
        first_group = where[0]
        if first_group and isinstance(first_group[0], tuple):
            first_col, first_op, first_val = first_group[0]
            first_col_short = first_col.split(".", 1)[1] if "." in first_col else first_col
    
            if first_op == "=":
                # Primary key lookup
                if first_col_short == pk and first_val in rows_by_pk:
                    candidate_keys = [first_val]
                # Index lookup
                elif first_col_short in indexes:
                    candidate_keys = indexes[first_col_short].get(first_val, [])
            elif first_op == "in" and isinstance(first_val, (list, tuple, set)):
                if first_col_short in indexes:
                    candidate_keys = []
                    for v in first_val:
                        candidate_keys += indexes[first_col_short].get(v, [])
    
        # ------------------------------------------------------------
        # Step 2: Build initial candidate rows
        # ------------------------------------------------------------
        if candidate_keys is not None:
            candidate_rows = [rows_by_pk[k] for k in candidate_keys if k in rows_by_pk]
        else:
            candidate_rows = list(rows_by_pk.values())
    
        # ------------------------------------------------------------
        # Step 3: Evaluate grouped AND/OR logic for each candidate row
        # ------------------------------------------------------------
        filtered = []
    
        def evaluate_group(row, group):
            if not group:
                return True
            logic = "AND"
            results = []
            for cond in group:
                if isinstance(cond, str) and cond.upper() in {"AND", "OR"}:
                    logic = cond.upper()
                    continue
                col, op, val = cond
                results.append(match_row(row, col, op, val))
            return (all(results) if logic == "AND" else any(results)), logic
    
        for row in candidate_rows:
            group_results, connectors = [], []
            for group in where:
                g_res, g_logic = evaluate_group(row, group)
                group_results.append(g_res)
                # connector between groups
                last = group[-1]
                connectors.append(last.upper() if isinstance(last, str) and last.upper() in {"AND", "OR"} else "OR")
    
            # combine group results sequentially
            res = group_results[0]
            for i in range(1, len(group_results)):
                conn = connectors[i - 1]
                res = (res and group_results[i]) if conn == "AND" else (res or group_results[i])
    
            if res:
                filtered.append(row)
    
        # ------------------------------------------------------------
        # Step 4: Return filtered list
        # ------------------------------------------------------------
        return filtered







    
    def group_by(self, rows, group_key, agg_col, agg_fn):
        from collections import defaultdict

        func_map = {
            "avg": lambda vals: round(sum(vals) / len(vals), 2) if vals else None,
            "sum": lambda vals: sum(vals) if vals else 0,
            "count": lambda vals: len(vals),
            "max": lambda vals: max(vals) if vals else None,
            "min": lambda vals: min(vals) if vals else None
        }
        if agg_fn not in func_map:
            valid = ", ".join(func_map.keys())
            print(f"Invalid agg_fn: {agg_fn}. Valid options are: {valid}")
            return []
            #raise ValueError(f"Invalid agg_fn: {agg_fn}. Valid options are: {valid}")
    
        agg_func = func_map[agg_fn]
        
        if group_key:
            grouped = defaultdict(list)
            for row in rows:
                grouped[row.get(group_key)].append(row.get(agg_col))
            table, col = agg_col.split(".", 1)
            return [
                {
                    group_key: k,
                    f"{table}.{agg_fn}_{col}": agg_func(
                        [v for v in vals if isinstance(v, (int, float))]
                    )
                }
                for k, vals in grouped.items()
            ]
        else:
            table, col = agg_col.split(".", 1)
            #print(table, col)
            #print(rows)
            #for row in rows:
            #    print(row.get(agg_col))
            values = [row.get(agg_col) for row in rows if isinstance(row.get(agg_col), (int, float))]
            return [{f"{table}.{agg_fn}_{col}": agg_func(values)}]

    
    def project_columns(self, rows, select):
        result = []
        for row in rows:
            new_row = {}
            for col in select:
                # Try exact match first
                if col in row:
                    new_row[col] = row[col]
                else:
                    # Try without prefix if it's missing
                    short_col = col.split('.')[-1]
                    match = next((v for k, v in row.items() if k.endswith(short_col)), None)
                    new_row[col] = match
            result.append(new_row)
        return result

     
        def sort_key(row):
            key = []
            for col, desc in zip(order_by, descending):
                val = row.get(col)
                if isinstance(val, (int, float)) and desc:
                    key.append(-val if val is not None else float('inf'))
                else:
                    # For descending on strings, invert with a tuple
                    key.append((val is None, val if not desc else _reverse_str(val)))
            return tuple(key)
        
        def _reverse_str(val):
            if isinstance(val, str):
                return ''.join(chr(255 - ord(c)) for c in val)
            return val
        
        return sorted(rows, key=sort_key)

    def order_by_rows(self, rows, order_by, descending=False):
        if not rows:
            return rows
    
        # Normalize inputs
        if isinstance(order_by, str):
            order_by = [order_by]
        if isinstance(descending, bool):
            descending = [descending] * len(order_by)
    
        # Apply sorts in reverse order (to preserve earlier priorities)
        for col, desc in reversed(list(zip(order_by, descending))):
            rows.sort(
                key=lambda r: (r.get(col) is None, r.get(col)),
                reverse=desc
            )
        return rows
    def reorder_conditions(self, conditions):
        reordered = []
        for group in conditions:
            new_group = []
            ops = []
            for item in group:
                if isinstance(item, str) and item.strip().upper() in ("AND", "OR"):
                    ops.append(item.strip().upper())
                else:
                    new_group.append(item)
            new_group.extend(ops)
            reordered.append(new_group)
        return reordered




    def _check_validate(self, from_table, joins=None, where=None, columns=None, agg_fn=None, agg_col=None, order_by=None):
        tables = [from_table] + [j[0] for j in joins or []]
        for t in tables:
            if t not in self.database:
                print(f"{self.MessageBGcolourS}Table not found: {t} {self.MessageBGcolourE}")
                return False
    
        def check_field(ref, context):
            if "." in ref:
                t, f = ref.split(".", 1)
                if t not in self.database:
                    print(f"{self.MessageBGcolourS}Table not found in {context}: {t} {self.MessageBGcolourE}")
                    return False
                sample = next(iter(self.database[t]['rows'].values()), {})
                if agg_fn and "_" in f:
                    prefix, actual = f.split("_", 1)
                    if prefix.lower() in {"sum", "avg", "count", "max", "min"}:
                        if actual not in sample:
                            print(f"{self.MessageBGcolourS}Field not found in {context}: {actual} {self.MessageBGcolourE}")
                            return False
                        return True
                if f not in sample:
                    print(f"{self.MessageBGcolourS}Field not found in {context}: {f} {self.MessageBGcolourE}")
                    return False
            return True
    
        # Validate agg_col always
        if agg_col and not check_field(agg_col, "agg_col"):
            return False
    
        # Validate columns
        for col in columns or []:
            if not check_field(col, "columns"):
                return False
    
        # Validate where (if not agg_fn)
        if not agg_fn:
            for lst in [c[0] for g in where or [] for c in g if isinstance(c, tuple)]:
                if not check_field(lst, "where"):
                    return False
    
        # Validate order_by
        for ob in order_by or []:
            if not check_field(ob, "order_by"):
                return False
    
        return True

        

    def select_query(self,
                     from_table,
                     joins=None,
                     where=None,
                     group_by=None,
                     agg_col=None,
                     agg_fn=None,
                     columns=None,
                     order_by=None,
                     descending=False):
        if where: where = self.reorder_conditions(where)
        if not self._check_validate(from_table, joins, where, columns, agg_fn, agg_col, order_by): return []
        rows = list(self.database[from_table]["rows"].values())
        rows = [{f"{from_table}.{k}": v for k, v in row.items()} for row in rows]
        base = from_table

        # Apply joins
        if joins:
            for join_table, on_keys, join_type in joins:
                left_key, right_key = on_keys
                if join_type == "inner":
                    rows = self.inner_join(base, join_table, left_key, right_key)
                elif join_type == "left":
                    rows = self.left_join(base, join_table, left_key, right_key)
                else:
                    print(f"{self.MessageBGcolourS}Only 'inner' and 'left' joins are supported. {self.MessageBGcolourE}")
                    return []
                    #raise ValueError("Only 'inner' and 'left' joins are supported.")
                base = "tmpTable"
                self.create_table(base, primary_key="id")
                for i, row in enumerate(rows):
                    row_with_id = {"id": i + 1}
                    row_with_id.update(row)
                    self.insert(base, row_with_id)

        # Apply WHERE
        # Apply WHERE
        if where:
            rows = self.select_where(base, where)   # ← use current base table (tmpTable after join)
            # Re-apply prefixes only if the base is the original table (when no join)
            if base == from_table:
                rows = [{f"{from_table}.{k}": v for k, v in row.items()} for row in rows]

        #print(rows[0:5])
        # Apply aggregation if specified or not
        if agg_col and agg_fn:
            rows = self.group_by(rows, group_by, agg_col, agg_fn)
       
        # Apply column selection
        if columns:
            rows = self.project_columns(rows, columns)
        if order_by:
            rows = self.order_by_rows(rows, order_by, descending)
        # Cleanup temporary table
        self.database.pop("tmpTable", None)
        return rows


# --- data_loader.py ---
#from csv_parser import CSVParser
#from my_custom_db import MyCustomDB



# --- main.py ---
#from csv_parser import CSVParser
#from my_custom_db import MyCustomMemoryDB
#from data_loader import DataLoader

def MyCustomMiniSQLEngine():
    parser = CSVParser()
    db = MyCustomMemoryDB()
    loader = DataLoader(db, parser)
    loader.load_all()
    return db

#MyCMSE = MyCustomMiniSQLEngine()

