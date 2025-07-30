# nlq_to_viz_workflow.py
from typing import Dict, Any, TypedDict, List
import ast, json
import pandas as pd

from router_agent import agent_2 as route_agents
from customer_agent import graph_final as customer_graph, d_store as AGENT_TABLES
from customer_helper import chain_filter_extractor, chain_query_extractor
from utils_parsing import parse_nested_list
from fuzzy_wuzzy import call_match as fuzzy_match_filters

from sql_viz_workflow import run_workflow as run_sql_viz  # validates SQL, executes, BI, viz gen/validate

class FinalState(TypedDict):
    question: str
    sql: str
    columns_selected: list
    filters_raw: Any
    filters_matched: Any
    result_debug_sql: str
    error_msg_debug_sql: str
    result_debug_python_code_data_visualization: str
    error_msg_debug_python_code_data_visualization: str
    df: pd.DataFrame
    visualization_request: str
    python_code_data_visualization: str
    python_code_store_variables_dict: dict

def _pick_tables_for_question(question: str) -> List[str]:
    raw = route_agents(question)  # e.g., "['customer','orders']"
    try:
        agents = ast.literal_eval(raw)
        if not isinstance(agents, list):
            agents = []
    except Exception:
        agents = []
    tables: List[str] = []
    for a in agents:
        tables.extend(AGENT_TABLES.get(a, []))
    if not tables:
        tables = AGENT_TABLES.get("orders", [])
    seen = set()
    deduped = [t for t in tables if not (t in seen or seen.add(t))]
    return deduped

def _subquestions_and_columns(question: str, tables: List[str]) -> List[list]:
    st = customer_graph.invoke({"user_query": question, "table_lst": tables})
    return st.get("column_extract", []) or []

def _filters(question: str, columns_selected: list):
    raw = chain_filter_extractor.invoke({
        "query": question,
        "columns": str(columns_selected)
    }).strip()
    as_list = parse_nested_list(raw)
    if as_list:
        matched = fuzzy_match_filters(as_list)
        return raw, matched
    return raw, raw

def _generate_sql(question: str, columns_selected: list, filters_any) -> str:
    filters_str = json.dumps(filters_any) if isinstance(filters_any, (list, dict)) else str(filters_any)
    sql = chain_query_extractor.invoke({
        "query": question,
        "columns": str(columns_selected),
        "filters": filters_str
    }).strip()
    return sql

def run(question: str, *, max_retries: int = 3) -> FinalState:
    tables = _pick_tables_for_question(question)
    columns_selected = _subquestions_and_columns(question, tables)
    filters_raw, filters_matched = _filters(question, columns_selected)
    sql = _generate_sql(question, columns_selected, filters_matched)
    state = run_sql_viz(
        question=question,
        sql=sql,
        columns=str(columns_selected),
        filters=json.dumps(filters_matched) if not isinstance(filters_matched, str) else filters_matched,
        max_retries=max_retries
    )
    combined: FinalState = {
        "question": question,
        "sql": state["sql"],
        "columns_selected": columns_selected,
        "filters_raw": filters_raw,
        "filters_matched": filters_matched,
        "df": state.get("df", pd.DataFrame()),
        "visualization_request": state.get("visualization_request", ""),
        "python_code_data_visualization": state.get("python_code_data_visualization", ""),
        "python_code_store_variables_dict": state.get("python_code_store_variables_dict", {}),
        "result_debug_sql": state.get("result_debug_sql", ""),
        "error_msg_debug_sql": state.get("error_msg_debug_sql", ""),
        "result_debug_python_code_data_visualization": state.get("result_debug_python_code_data_visualization",""),
        "error_msg_debug_python_code_data_visualization": state.get("error_msg_debug_python_code_data_visualization",""),
    }
    return combined
