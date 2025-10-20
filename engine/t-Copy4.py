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
    enable_join = st.checkbox("🔗 Enable Join", value=False)
    enable_where = st.checkbox("🔗 Enable Where", value=False)
    st.markdown("### TABLES")
    for table, meta in db.database.items():
        with st.expander(f"📂 {table}", expanded=False):
            for col in meta['rows'][next(iter(meta['rows']))].keys():
                st.markdown(f"- {col}")

# ------------------------ MAIN PANEL ------------------------
st.markdown("## Custom SQL Query Builder")

col1, col2 = st.columns([1, 3])

with col1:
    # FROM TABLE
    from_table = st.selectbox("FROM", list(db.database.keys()))

    # ✅ OPTIONAL JOIN SECTION
   

    join_table = left_key = right_key = join_type = None
    if enable_join:
        st.markdown("### JOIN")
        join_table = st.selectbox("Join Table", list(db.database.keys()))
        left_key = st.selectbox("Left Key", db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
        right_key = st.selectbox("Right Key", db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
        join_type = st.radio("Join Type", ["inner", "left"], horizontal=True)

    # WHERE
    where_col = where_op = where_val = None
    if enable_where:
        st.markdown("### WHERE")
        where_col = st.selectbox("Column", db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
        where_op = st.selectbox("Operator", ["=", "!=", ">", "<", ">=", "<="])
        where_val = st.text_input("Value")

    # GROUP BY
    st.markdown("### GROUP BY")
    group_by = st.selectbox("Group By", ["None"] + list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys()))
    agg_fn = st.selectbox("Aggregation", ["avg", "sum", "count", "max", "min", "None"])

    # ✅ Use correct source for aggregation column
    agg_source_table = join_table if enable_join else from_table
    agg_col = st.selectbox("Agg Column", list(db.database[agg_source_table]['rows'][next(iter(db.database[agg_source_table]['rows']))].keys()))

    # ORDER BY
    st.markdown("### ORDER BY")
    order_cols = list(db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys())
    if enable_join:
        order_cols += list(db.database[join_table]['rows'][next(iter(db.database[join_table]['rows']))].keys())
    order_by = st.multiselect("Order By Columns", order_cols)
    descending = st.checkbox("Descending")

    # RUN QUERY
    if st.button("Run Query"):
        joins = [(join_table, (left_key, right_key), join_type)] if enable_join else None
        where = [[(f"{from_table}.{where_col}", where_op, where_val)]] if where_col and where_val else None
        group_by_prefixed = f"{from_table}.{group_by}" if group_by and group_by != "None" else None
        agg_fn_used = agg_fn if agg_fn != "None" else None
        agg_col_prefixed = f"{agg_source_table}.{agg_col}" if agg_col else None

        # Select columns
        select_cols = []
        if group_by and group_by != "None":
            select_cols.append(f"{from_table}.{group_by}")
        if agg_fn_used:
            select_cols.append(f"{agg_source_table}.{agg_fn_used}_{agg_col}")
        else:
            select_cols += [f"{from_table}.{col}" for col in db.database[from_table]['rows'][next(iter(db.database[from_table]['rows']))].keys()]

        # Execute query
        result = db.select_query(
            from_table=from_table,
            joins=joins,
            where=where,
            group_by=group_by_prefixed,
            agg_col=agg_col_prefixed,
            agg_fn=agg_fn_used,
            columns=select_cols,
            order_by=[f"{agg_source_table}.{col}" if col in db.database[agg_source_table]['rows'][next(iter(db.database[agg_source_table]['rows']))].keys() else f"{from_table}.{col}" for col in order_by],
            descending=[descending] * len(order_by)
        )

        # Display result
        df = pd.DataFrame(result)
        st.dataframe(df)
