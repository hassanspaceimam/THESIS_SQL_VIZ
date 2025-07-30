# streamlit_chat.py
import json
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from nlq_to_viz_workflow import run as run_full

st.set_page_config(page_title="SQL/BI Agent", layout="wide")
st.title("📊 SQL And Visualization Generator")
st.markdown("Type a question in English. I’ll generate the SQL, run it, and show the best visualization.")

question = st.text_input("Your question", placeholder="e.g., What is the monthly trend of total sales?")

with st.expander("Advanced (optional)"):
    max_retries = st.number_input("Max retries (SQL & Viz)", min_value=0, max_value=6, value=3, step=1)

if st.button("Run", type="primary"):
    if not question.strip():
        st.warning("Please enter a question.")
    else:
        with st.spinner("Thinking, generating SQL, validating, and visualizing…"):
            state = run_full(question, max_retries=max_retries)

        c1, c2 = st.columns([0.45, 0.55])
        with c1:
            st.subheader("Generated SQL")
            st.code(state["sql"], language="sql")

            sql_text = state.get("sql", "") or ""
            col_sql_1, col_sql_2 = st.columns(2)
            with col_sql_1:
                st.download_button(
                    "Download SQL",
                    data=sql_text.encode("utf-8"),
                    file_name="query.sql",
                    mime="text/sql",
                    use_container_width=True
                )
            with col_sql_2:
                escaped_sql = json.dumps(sql_text)
                components.html(
                    f"""
                    <div style="display:flex;gap:8px;">
                      <button
                        style="width:100%;padding:0.5rem 0.75rem;border:1px solid #ddd;border-radius:6px;cursor:pointer;background:#f6f6f6;"
                        onclick="navigator.clipboard.writeText({escaped_sql}).then(() => {{
                          const el = this; const old = el.innerText; el.innerText='Copied!';
                          setTimeout(() => el.innerText=old, 1200);
                        }})"
                      >Copy SQL</button>
                    </div>
                    """,
                    height=50
                )

            st.caption("Selected columns (from agents)")
            st.write(state["columns_selected"])

            st.caption("Filters (raw → matched)")
            st.code(str(state["filters_raw"]))
            st.code(str(state["filters_matched"]))

            st.subheader("BI Expert Recommendation")
            st.write(state["visualization_request"])

            st.subheader("Generated Python (Plotly)")
            st.code(state.get("python_code_data_visualization", ""), language="python")
            if state.get("result_debug_python_code_data_visualization") == "Not Pass":
                st.error(state.get("error_msg_debug_python_code_data_visualization", ""))

            st.subheader("SQL Validation")
            st.markdown(f"**Status:** {state.get('result_debug_sql','')}")
            if state.get("error_msg_debug_sql"):
                st.error(state["error_msg_debug_sql"])

        with c2:
            st.subheader("Result")
            d = state.get("python_code_store_variables_dict", {}) or {}
            fig = d.get("fig")
            df_viz = d.get("df_viz")
            text_v = d.get("string_viz_result")

            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
            elif isinstance(df_viz, pd.DataFrame):
                st.dataframe(df_viz, use_container_width=True)
            elif text_v:
                st.markdown(text_v)
            else:
                st.info("No figure/table/text produced by the visualization code.")

            download_df = None
            if isinstance(df_viz, pd.DataFrame) and not df_viz.empty:
                download_df = df_viz
            elif isinstance(state.get("df"), pd.DataFrame) and not state["df"].empty:
                download_df = state["df"]

            if download_df is not None:
                csv_bytes = download_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download results (CSV)",
                    data=csv_bytes,
                    file_name="results.csv",
                    mime="text/csv",
                    use_container_width=True
                )
