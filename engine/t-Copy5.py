import streamlit as st
import pandas as pd
from main import MyCustomMiniSQLEngine

st.set_page_config(layout="wide")
st.markdown("""
    <style>
        /* Make main container full width */
        .main {
            background-color: white;
        }

        /* Make data frame take 100% width */
        [data-testid="stDataFrame"] {
            width: 100% !important;
        }

        /* Remove inner container scroll and constraints */
        .element-container:has([data-testid="stDataFrame"]) {
            padding-right: 0 !important;
            margin-right: 0 !important;
        }

        /* Expand layout where horizontal blocks are used (e.g., columns) */
        [data-testid="stHorizontalBlock"] {
            align-items: stretch !important;
        }

        /* Force inner block of dataframe to stretch */
        .stDataFrame div[data-testid="stVerticalBlock"] {
            width: 100% !important;
        }
    </style>
""", unsafe_allow_html=True)



# ------------------------ LOAD DATABASE ------------------------
@st.cache_resource
def load_db():
    return MyCustomMiniSQLEngine()

db = load_db()

# ------------------------ LEFT SIDEBAR ------------------------
with st.sidebar:
    enable_join = st.checkbox("🔗 Enable Join", value=False)
    enable_Aggregation = st.checkbox("🔗 Enable Aggregate", value=False)
    enable_where = st.checkbox("🔗 Enable Where", value=False)
    enable_order_by = st.checkbox("🔗 Enable Order By", value=False)
    st.markdown("### TABLES")
    for table, meta in db.database.items():
        with st.expander(f"📂 {table}", expanded=False):
            for col in meta['rows'][next(iter(meta['rows']))].keys():
                st.markdown(f"- {col}")

# ------------------------ MAIN PANEL ------------------------
st.markdown("## Custom SQL Query Builder")

col1, col2 = st.columns([1, 1])

with col1:
    # FROM TABLE
    from_table = st.selectbox("FROM", list(db.database.keys()))

    # ✅ OPTIONAL JOIN SECTION
    join_table = left_key = right_key = join_type = None
    if enable_join:
        st.markdown("### JOIN")
        joinable_tables = [tbl for tbl in db.database.keys() if tbl != from_table]
        join_table = st.selectbox("Join Table", joinable_tables)
        left_key = st.selectbox("Left Key", db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
        right_key = st.selectbox("Right Key", db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
        join_type = st.radio("Join Type", ["inner", "left"], horizontal=True)

    # ✅ WHERE
    where_col = where_op = where_val = None
    if enable_where:
        st.markdown("### WHERE")
        wcol, wop, wval = st.columns([2, 1, 3])
        
        with wcol:
            where_col = st.selectbox("Column", db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
        
        with wop:
            where_op = st.selectbox("Operator", ["=", "!=", ">", "<", ">=", "<="])
        
        with wval:
            raw_where_val = st.text_input("Value")
        
            # Try to convert to number if possible
            try:
                if "." in raw_where_val:
                    where_val = float(raw_where_val)
                else:
                    where_val = int(raw_where_val)
            except:
                where_val = raw_where_val  # keep as string if not number

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
            agg_source_table = join_table if enable_join else from_table
            agg_col = st.selectbox(
                "Agg Column",
                list(db.database[agg_source_table]['rows'][next(iter(db.database[agg_source_table]['rows']))].keys())
            )
    # ✅ COLUMNS SELECTION
    st.markdown("### COLUMNS")
    all_fields = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
    if enable_join:
        all_fields += list(db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
    selected_columns = st.multiselect("Select Columns", options=all_fields, default=all_fields)
    
    # ✅ GROUP BY & AGGREGATION
    group_by = None
    if enable_Aggregation: 
        st.markdown("### GROUP BY")
        group_by = st.selectbox(
        "Group By",
        ["None"] + list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys()),
        index=0  # 🔹 Ensures "None" is selected by default
        )






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
    where = [[(f"{from_table}.{where_col}", where_op, where_val)]] if where_col and where_val else None
    group_by_prefixed = f"{from_table}.{group_by}" if group_by and group_by != "None" else None
    agg_fn_used = agg_fn if agg_fn != "None" else None
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
