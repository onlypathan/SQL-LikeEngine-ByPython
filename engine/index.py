import streamlit as st
import pandas as pd
from main import MyCustomMiniSQLEngine
import ast, re

st.set_page_config(layout="wide")

# ------------------------ LOAD DATABASE ------------------------
@st.cache_resource
def load_db():
    return MyCustomMiniSQLEngine()

db = load_db()


# ------------------------ SIDEBAR ------------------------
with st.sidebar:
    st.markdown("### ENABLE")
    enable_join = st.checkbox("🔗 Join", value=False)
    enable_Aggregation = st.checkbox("🔗 Aggregate", value=False)
    enable_where = st.checkbox("🔗 Where", value=False)
    enable_order_by = st.checkbox("🔗 Order By", value=False)

    st.markdown("### TABLES")
    for table, meta in db.database.items():
        with st.expander(f"📂 {table}", expanded=False):
            for col in meta['rows'][next(iter(meta['rows']))].keys():
                # ✅ In advanced mode, columns are clickable to insert text
                if st.session_state.get("advanced_mode", False):
                    if st.button(f"{col}", key=f"{table}_{col}"):
                        # ✅ Ensure the input exists
                        if "adv_query_input" not in st.session_state:
                            st.session_state.adv_query_input = ""
                
                        # ✅ Append the column reference to the current text box content
                        current_text = st.session_state.adv_query_input
                        st.session_state.adv_query_input = current_text + f'"{table}.{col}" '
                        
                        # ✅ Rerun to update text box with new content
                        st.rerun()

                else:
                    st.markdown(f"- {col}")


# ------------------------ MAIN PANEL HEADER ------------------------
col_main, col_adv = st.columns([5, 1.5])
with col_main:
    st.markdown("## 🧩 Custom SQL-Like Query Builder - By Python")
with col_adv:
    if "advanced_mode" not in st.session_state:
        st.session_state.advanced_mode = False

    toggle_label = "🧠 Advanced Query Mode" if not st.session_state.advanced_mode else "↩️ Normal Query Mode"
    if st.button(toggle_label, use_container_width=True):
        st.session_state.advanced_mode = not st.session_state.advanced_mode
        st.rerun()


# ------------------------ ADVANCED QUERY MODE ------------------------
if st.session_state.advanced_mode:
    # ------------------------ ADVANCED QUERY MODE ------------------------
    st.markdown("### 🧠 Advanced Query Editor")
    st.info("Click any table/column name on the left to insert it into your query.")
    
    # ✅ Default query string
    default_query = '''select_query(
        from_table="restaurant_info",
        joins=[("inspection_info", ("Restaurant_Info_ID", "F_Restaurant_Info_ID"), "inner")],
        where=[
            [("restaurant_info.Categories", "=", "Mexican"),
             "or", ("restaurant_info.Review_Count", ">", 5000)]
        ],
        group_by="restaurant_info.Categories",
        agg_col="inspection_info.Score",
        agg_fn="avg",
        columns=["restaurant_info.Categories", "inspection_info.avg_Score"],
        order_by=["inspection_info.avg_Score", "restaurant_info.Categories"],
        descending=[True, True]
    )'''
    
    # ✅ Initialize session state variables
    if "adv_query_input" not in st.session_state:
        st.session_state.adv_query_input = default_query
    
    # ✅ RESET TRIGGER — we check this BEFORE rendering the widget
    if st.session_state.get("reset_trigger", False):
        # We reset the text area value before rendering
        st.session_state.adv_query_input = default_query
        st.session_state.reset_trigger = False
        st.rerun()
    
    # ✅ Render the text area
    query_input = st.text_area(
        "Advanced Query Input",
        height=400,
        key="adv_query_input"
    )
    
    # ------------------- RUN QUERY + RESET BUTTONS -------------------
    run_col, reset_col = st.columns([5.5, 1])
    
    result = None
    with run_col:
        if st.button("🚀 Run Query"):
            import traceback
            try:
                result = eval(st.session_state.adv_query_input, {"select_query": db.select_query})
    
                if result is None:
                    st.error("⚠️ `db.select_query()` returned None.")
                    st.stop()
    
                if not isinstance(result, (list, tuple)):
                    st.error(f"⚠️ Unexpected result type: {type(result)}")
                    st.write(result)
                    st.stop()
    
                if not result:
                    st.warning("⚠️ Query executed successfully but returned no rows.")
                    st.stop()
    
            except Exception as e:
                st.error(f"❌ Query failed: {e}")
                st.code(traceback.format_exc())
    
    with reset_col:
        if st.button("🔄 Reset Input"):
            st.session_state.reset_trigger = True  # ✅ Set trigger
            st.rerun()
    
    # ✅ Show result if query was successful
    if result:
        df = pd.DataFrame(result)
        st.markdown("## Query Result")
        st.dataframe(df, use_container_width=True)
    
    # ✅ Stop page to prevent loading normal builder
    st.stop()




# ------------------------ NORMAL QUERY BUILDER ------------------------
col1, col2 = st.columns([2, 1])

with col1:
    # FROM TABLE
    if not enable_join:
        from_table = st.selectbox("FROM", list(db.database.keys()))

    # ✅ OPTIONAL JOIN SECTION
    join_table = left_key = right_key = join_type = None
    if enable_join:
        st.markdown("### JOIN")
    
        from_col, left_key_col = st.columns([1, 1])
        with from_col:
            from_table = st.selectbox("FROM", list(db.database.keys()), key="from_table")
        with left_key_col:
            left_key = st.selectbox(
                "Left Key", 
                db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys(),
                key="left_key"
            )
    
        joinable_tables = [tbl for tbl in db.database.keys() if tbl != from_table]
    
        join_col, right_key_col = st.columns([1, 1])
        with join_col:
            join_table = st.selectbox("Join Table", joinable_tables, key="join_table")
        with right_key_col:
            right_key = st.selectbox(
                "Right Key (Join On)", 
                db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys(),
                key="right_key (Join On)"
            )
    
        # ✅ Inline Join Type
        join_type = st.radio("Join Type", ["inner", "left"], horizontal=True)

    
    # ✅ WHERE SECTION
    def _infer_type(value: str):
        if value is None or str(value).strip() == "":
            return None
        txt = str(value).strip()
        try:
            if "." in txt:
                return float(txt)
            return int(txt)
        except ValueError:
            return txt

    if enable_where:
        st.markdown("### WHERE")
    
        if "from_table" not in locals() or not from_table:
            st.warning("⚠️ Please select a table first to use WHERE filters.")
        else:
            all_columns = list(db.database[from_table]['rows']
                               [next(iter(db.database[from_table]['rows']))].keys())
            if enable_join and join_table:
                all_columns += list(db.database[join_table]['rows']
                                    [next(iter(db.database[join_table]['rows']))].keys())

            if "where_conditions" not in st.session_state or not st.session_state.where_conditions:
                st.session_state.where_conditions = [{
                    "logic": "AND",
                    "col": all_columns[0],
                    "op": "=",
                    "val": ""
                }]
    
            new_conditions = []
            for i, cond in enumerate(st.session_state.where_conditions):
                cols = st.columns([1, 2, 1, 2])
    
                if i == 0:
                    logic = "AND"
                    cols[0].markdown("**Logic**")
                    cols[0].markdown("—")
                else:
                    logic = cols[0].selectbox(
                        "Logic", ["AND", "OR"],
                        key=f"logic_{i}",
                        index=["AND", "OR"].index(cond["logic"])
                    )
    
                try:
                    col_index = all_columns.index(cond["col"])
                except ValueError:
                    col_index = 0
                col = cols[1].selectbox("Column", all_columns,
                                        key=f"col_{i}", index=col_index)
    
                op = cols[2].selectbox("Operator",
                                       ["=", "!=", ">", "<", ">=", "<="],
                                       key=f"op_{i}",
                                       index=["=", "!=", ">", "<", ">=", "<="].index(cond["op"]))
    
                raw = cols[3].text_input("Value", value=str(cond.get("val", "")), key=f"val_{i}")
                val = _infer_type(raw)
    
                new_conditions.append({"logic": logic, "col": col, "op": op, "val": val})
    
            c1, c2 = st.columns([8, 1])
            with c2:
                if st.button("➕ Add"):
                    st.session_state.where_conditions.append({
                        "logic": "AND",
                        "col": all_columns[0],
                        "op": "=",
                        "val": ""
                    })
                    st.rerun()
    
            st.session_state.where_conditions = new_conditions
    
    else:
        st.session_state.where_conditions = []


    # ✅ AGGREGATION
    agg_fn = agg_col = None
    if enable_Aggregation: 
        st.markdown("### AGGREGATE")
        agg_fn = st.selectbox(
            "Aggregation",
            ["None", "avg", "sum", "count", "max", "min"],
            index=0
        )
        
        agg_col = None
        if agg_fn != "None":
            st.markdown("### Agg Column")
            agg_fields = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
            if enable_join:
                agg_fields += list(db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
            agg_col = st.selectbox("Agg Column", agg_fields)


    # ✅ GROUP BY
    group_by = None
    if enable_Aggregation:
        st.markdown("### GROUP BY")
        group_by_fields = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
        if enable_join:
            group_by_fields += list(db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
        group_by = st.selectbox("Group By", ["None"] + group_by_fields, index=0)


    # ✅ COLUMNS
    st.markdown("### COLUMNS")
    all_fields = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
    if enable_join:
        all_fields += list(db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
    selected_columns = st.multiselect("Select Columns", options=all_fields, default=all_fields)


    # ✅ ORDER BY
    order_by = None
    if enable_order_by: 
        st.markdown("### ORDER BY")
        order_cols = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
        if enable_join:
            order_cols += list(db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
        order_by = st.multiselect("Order By Columns", order_cols)
        descending_flags = {col: st.checkbox(f"Descending: {col}", key=f"desc_{col}") for col in order_by}


# ✅ RUN QUERY (Normal Mode)
try:
    if st.button("Run Query"):
        joins = [(join_table, (left_key, right_key), join_type)] if enable_join else None
    
        where = []
        for i, cond in enumerate(st.session_state.where_conditions):
            if enable_join and cond['col'] in db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys():
                full_col = f"{join_table}.{cond['col']}"
            else:
                full_col = f"{from_table}.{cond['col']}"

            clause = (full_col, cond['op'], cond['val'])
            if i == 0:
                where.append([clause])
            else:
                if cond["logic"] == "AND":
                    where[-1].append(clause)
                else:
                    where.append([clause])
    
        group_by_prefixed = f"{from_table}.{group_by}" if group_by and group_by != "None" else None
        agg_fn_used = agg_fn if agg_fn != "None" else None
        agg_source_table = join_table if enable_join and agg_col in db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys() else from_table
        agg_col_prefixed = f"{agg_source_table}.{agg_col}" if agg_col else None
    
        select_cols = []
        if group_by and group_by != "None":
            select_cols.append(f"{from_table}.{group_by}")
        if agg_fn_used:
            select_cols.append(f"{agg_source_table}.{agg_fn_used}_{agg_col}")
        select_cols += [
            f"{join_table}.{col}" if enable_join and col in db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys()
            else f"{from_table}.{col}"
            for col in selected_columns
            if col != group_by and col != agg_col
        ]
    
        result = db.select_query(
            from_table=from_table,
            joins=joins,
            where=where,
            group_by=group_by_prefixed,
            agg_col=agg_col_prefixed,
            agg_fn=agg_fn_used,
            columns=select_cols,
            order_by=[
                f"{agg_source_table}.{col}" if col in db.database[agg_source_table]['rows'][next(iter(db.database[agg_source_table]['rows']))].keys()
                else f"{from_table}.{col}" for col in order_by
            ] if order_by else None,
            descending=[descending_flags[col] for col in order_by] if order_by else None
        )
    
        df = pd.DataFrame(result)
        st.markdown("## Query Result")
        st.dataframe(df, use_container_width=True)

except Exception as e:    
    st.error(f"Query failed: {e}")
