import streamlit as st
import pandas as pd
from main import MyCustomMiniSQLEngine

st.set_page_config(layout="wide")




# ------------------------ LOAD DATABASE ------------------------
@st.cache_resource
def load_db():
    return MyCustomMiniSQLEngine()

db = load_db()

# ------------------------ LEFT SIDEBAR ------------------------
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
                st.markdown(f"- {col}")

# ------------------------ MAIN PANEL ------------------------
st.markdown("## Custom SQL Query Builder")

col1, col2 = st.columns([2, 1])

with col1:
    # FROM TABLE
    if not enable_join:
        from_table = st.selectbox("FROM", list(db.database.keys()))

    # ✅ OPTIONAL JOIN SECTION
    join_table = left_key = right_key = join_type = None
    if enable_join:
        st.markdown("### JOIN")
    
        # 1st row: FROM + Left Key
        from_col, left_key_col = st.columns([1, 1])
        with from_col:
            from_table = st.selectbox("FROM", list(db.database.keys()), key="from_table")
        with left_key_col:
            left_key = st.selectbox(
                "Left Key", 
                db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys(),
                key="left_key"
            )
    
        # Prepare join table options excluding from_table
        joinable_tables = [tbl for tbl in db.database.keys() if tbl != from_table]
    
        # 2nd row: Join Table + Right Key
        join_col, right_key_col = st.columns([1, 1])
        with join_col:
            join_table = st.selectbox("Join Table", joinable_tables, key="join_table")
        with right_key_col:
            right_key = st.selectbox(
                "Right Key (Join On)", 
                db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys(),
                key="right_key (Join On)"
            )
    
        # Join Type Radio
        join_type = st.radio("Join Type", ["inner", "left"], horizontal=True)


    # ✅ WHERE
    # ✅ WHERE
    where = None

    if not enable_where:
        st.markdown("### WHERE")
    
        all_columns = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
    
        # Initialize WHERE session state
        if "where_conditions" not in st.session_state:
            st.session_state.where_conditions = [{
                "logic": "AND",  # This will be ignored for the first row
                "col": all_columns[0],
                "op": "=",
                "val": ""
            }]
    
        new_conditions = []
    
        for i, cond in enumerate(st.session_state.where_conditions):
            cols = st.columns([1, 2, 1, 2])
    
            # 1️⃣ Logic (AND/OR) – skip for first row
            if i == 0:
                logic = "AND"  # ignored
                cols[0].markdown("**Logic**")
                cols[0].markdown("—")
            else:
                logic = cols[0].selectbox("Logic", ["AND", "OR"], key=f"where_logic_{i}", index=["AND", "OR"].index(cond["logic"]))
    
            # 2️⃣ Column
            try:
                col_index = all_columns.index(cond["col"])
            except ValueError:
                col_index = 0  # fallback to first column if old column not found
            
            col = cols[1].selectbox("Column", all_columns, key=f"where_col_{i}", index=col_index)

    
            # 3️⃣ Operator
            op = cols[2].selectbox("Operator", ["=", "!=", ">", "<", ">=", "<="], key=f"where_op_{i}", index=["=", "!=", ">", "<", ">=", "<="].index(cond["op"]))
    
            # 4️⃣ Value
            val_input = cols[3].text_input("Value", value=cond["val"], key=f"where_val_{i}")
    
            # Try to convert value to number if applicable
            try:
                val = float(val_input) if "." in val_input else int(val_input)
            except:
                val = val_input
    
            new_conditions.append({"logic": logic, "col": col, "op": op, "val": val})
    
        # ➕ Add button aligned to last row
        add_col = st.columns([9, 1])
        if add_col[1].button("Add", key="add_where_row"):
            new_conditions.append({
                "logic": "AND",
                "col": all_columns[0],
                "op": "=",
                "val": ""
            })
    
        # Save back updated conditions
        st.session_state.where_conditions = new_conditions




    # ✅ AGGREGATION
    agg_fn = agg_col = None
    if enable_Aggregation: 
        # Aggregation
        st.markdown("### AGGREGATE")
        agg_fn = st.selectbox(
            "Aggregation",
            ["None", "avg", "sum", "count", "max", "min"],
            index=0
        )
        
        agg_col = None
        if agg_fn != "None":
            st.markdown("### Agg Column")
            
            # ✅ Combine fields from both tables
            agg_fields = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
            if enable_join:
                agg_fields += list(db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
        
            agg_col = st.selectbox("Agg Column", agg_fields)

    
    # ✅ GROUP BY & AGGREGATION
    group_by = None
    if enable_Aggregation: 
        st.markdown("### GROUP BY")
        group_by = st.selectbox(
        "Group By",
        ["None"] + list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys()),
        index=0  # 🔹 Ensures "None" is selected by default
        )

 # ✅ COLUMNS SELECTION
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
        descending_flags = {}
        
        for col in order_by:
            descending_flags[col] = st.checkbox(f"Descending: {col}", key=f"desc_{col}")

    # ✅ RUN QUERY
if st.button("Run Query"):
    joins = [(join_table, (left_key, right_key), join_type)] if enable_join else None
    # ✅ Convert to proper WHERE format and handle numeric conversion
    where = []

    for i, cond in enumerate(st.session_state.where_conditions):
        full_col = f"{from_table}.{cond['col']}"
        clause = (full_col, cond['op'], cond['val'])

        if i == 0:
            where.append([clause])  # first group
        else:
            if cond["logic"] == "AND":
                where[-1].append(clause)  # append to current group
            else:
                where.append([clause])  # start new group


    group_by_prefixed = f"{from_table}.{group_by}" if group_by and group_by != "None" else None
    agg_fn_used = agg_fn if agg_fn != "None" else None
    agg_source_table = join_table if enable_join and agg_col in db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys() else from_table
    agg_col_prefixed = f"{agg_source_table}.{agg_col}" if agg_col else None

    # Build selected columns
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
    try:
        # Execute query
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
    
        # ✅ Show result table immediately after result is computed
        df = pd.DataFrame(result)
        st.markdown("## Query Result")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Query failed: {e}")
