# sql_viz_workflow.py
from typing import TypedDict, Dict, Any
from langgraph.graph import StateGraph, START, END
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from sqlalchemy import text
import pandas as pd
import re
import traceback

from config import get_llm, get_engine
from prompts import (
    system_prompt_agent_bi_expert_node,
    system_prompt_agent_python_code_data_visualization_generator_node,
    system_prompt_agent_python_code_data_visualization_validator_node,
)
from utils import extract_code_block

# Engine & LLM centralized (behavior unchanged)
engine = get_engine()
llm = get_llm()

class AgentState(TypedDict):
    question: str
    sql: str
    columns: str
    filters: str
    num_retries_debug_sql: int
    max_num_retries_debug: int
    result_debug_sql: str
    error_msg_debug_sql: str
    df: pd.DataFrame
    visualization_request: str
    python_code_data_visualization: str
    num_retries_debug_python_code_data_visualization: int
    result_debug_python_code_data_visualization: str
    error_msg_debug_python_code_data_visualization: str
    python_code_store_variables_dict: dict

def _only_select(sql: str) -> None:
    if not re.match(r"(?is)^\s*select\b", sql or ""):
        raise ValueError("Only SELECT statements are allowed.")

def _wrap_with_limit(sql: str, limit: int = 2000) -> str:
    s = (sql or "").strip().rstrip(";")
    if re.search(r"(?is)\blimit\s+\d+\s*$", s):
        return s
    return f"SELECT * FROM ({s}) AS t LIMIT {limit}"

def _explain_safe(sql: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text("EXPLAIN " + sql))
    except Exception:
        pass

_sql_fixer_prompt = ChatPromptTemplate.from_messages([
    ("system", """
You are a precise MySQL query fixer.

STRICT OUTPUT:
- Return ONLY a single corrected MySQL SELECT statement. No prose, no markdown.

CONSTRAINTS:
- Use ONLY tables/columns implied by the provided "Relevant context" if present.
- Apply filters exactly when provided (do not invent or omit).
- If city/state is referenced for customers, it comes from customer.customer_city / customer.customer_state via orders.customer_id = customer.customer_id.
- Avoid reserved words as aliases. Balance parentheses. If using CTEs, ensure full WITH clauses (MySQL 8+).

"""),
    ("human", """
User question:
{question}

Relevant context (optional; may be empty):
Columns:
{columns}

Filters:
{filters}

Current SQL to fix:
{sql}

Database error message:
{error}
""")
])
_sql_fixer_chain = _sql_fixer_prompt | llm | StrOutputParser()

def sql_validate_and_execute_node(state: AgentState) -> AgentState:
    sql_in = (state.get("sql") or "").strip()
    if not sql_in:
        raise ValueError("No SQL provided to the validator. Pass sql=... or generate one before this step.")

    for attempt in range(state["num_retries_debug_sql"], state["max_num_retries_debug"] + 1):
        try:
            _only_select(sql_in)
            limited_sql = _wrap_with_limit(sql_in, limit=2000)
            _explain_safe(limited_sql)

            df = pd.read_sql(text(limited_sql), con=get_engine())
            state["df"] = df
            state["result_debug_sql"] = "Pass"
            state["error_msg_debug_sql"] = ""
            state["sql"] = sql_in
            return state

        except Exception as e:
            state["num_retries_debug_sql"] = attempt + 1
            state["result_debug_sql"] = "Not Pass"
            tb = traceback.format_exc(limit=1)
            err_short = (str(e) + " | " + tb)[:600]
            state["error_msg_debug_sql"] = err_short

            sql_in = _sql_fixer_chain.invoke({
                "question": state["question"],
                "columns": state.get("columns", ""),
                "filters": state.get("filters", ""),
                "sql": sql_in,
                "error": err_short
            }).strip()
    return state

def bi_expert_node(state: AgentState) -> AgentState:
    prompt = ChatPromptTemplate.from_messages([("system", system_prompt_agent_bi_expert_node)])
    chain = prompt | llm | StrOutputParser()
    df = state.get("df", pd.DataFrame())
    response = chain.invoke({
        "question": state["question"],
        "query": state["sql"],
        "df_structure": df.dtypes if not df.empty else "EMPTY",
        "df_sample": df.head(5) if not df.empty else "EMPTY"
    }).strip()
    state["visualization_request"] = response
    return state

def viz_code_generator_node(state: AgentState) -> AgentState:
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt_agent_python_code_data_visualization_generator_node)
    ])
    chain = prompt | llm | StrOutputParser()
    df = state.get("df", pd.DataFrame())
    response = chain.invoke({
        "visualization_request": state["visualization_request"],
        "df_structure": df.dtypes if not df.empty else "EMPTY",
        "df_sample": df.head(5) if not df.empty else "EMPTY"
    })
    state["python_code_data_visualization"] = extract_code_block(response, "python").strip()
    return state

def viz_code_validator_node(state: AgentState) -> AgentState:
    code = state.get("python_code_data_visualization", "").strip()
    if not code:
        state["result_debug_python_code_data_visualization"] = "Not Pass"
        state["error_msg_debug_python_code_data_visualization"] = "Empty python visualization code."
        return state

    for attempt in range(state["num_retries_debug_python_code_data_visualization"], state["max_num_retries_debug"] + 1):
        try:
            import pandas as pd
            import plotly.express as px
            import plotly.graph_objects as go
            import re

            # FIX: Avoid evaluating a DataFrame in a boolean context
            df = state.get("df")
            if df is None:
                df = pd.DataFrame()

            code_to_run = re.sub(r"state\.get\(\s*['\"]df['\"]\s*\)", "df", code)
            code_to_run = re.sub(r"fig\.show\(\)\s*;?", "", code_to_run)

            exec_globals: Dict[str, Any] = {"df": df, "pd": pd, "px": px, "go": go, "state": {"df": df}}
            exec(code_to_run, exec_globals)

            state["python_code_store_variables_dict"] = exec_globals
            state["result_debug_python_code_data_visualization"] = "Pass"
            state["error_msg_debug_python_code_data_visualization"] = ""
            state["python_code_data_visualization"] = code_to_run
            return state

        except Exception as e:
            import traceback
            state["num_retries_debug_python_code_data_visualization"] = attempt + 1
            state["result_debug_python_code_data_visualization"] = "Not Pass"
            err_short = (str(e) + " | " + traceback.format_exc(limit=1))[:800]
            state["error_msg_debug_python_code_data_visualization"] = err_short

            from langchain_core.prompts import ChatPromptTemplate
            from langchain_core.output_parsers import StrOutputParser
            prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt_agent_python_code_data_visualization_validator_node)
            ])
            chain = prompt | llm | StrOutputParser()
            fixed = chain.invoke({
                "python_code_data_visualization": code,
                "error_msg_debug": err_short
            })
            from utils import extract_code_block
            code = extract_code_block(fixed, "python").strip()
    return state

graph = StateGraph(AgentState)
graph.add_node("sql_validate_and_execute", sql_validate_and_execute_node)
graph.add_node("bi_expert", bi_expert_node)
graph.add_node("viz_code_generator", viz_code_generator_node)
graph.add_node("viz_code_validator", viz_code_validator_node)

graph.add_edge(START, "sql_validate_and_execute")
graph.add_edge("sql_validate_and_execute", "bi_expert")
graph.add_edge("bi_expert", "viz_code_generator")
graph.add_edge("viz_code_generator", "viz_code_validator")
graph.add_edge("viz_code_validator", END)

app = graph.compile()

def run_workflow(
    question: str,
    sql: str,
    *,
    columns: str = "",
    filters: str = "",
    max_retries: int = 3
) -> AgentState:
    initial: AgentState = {
        "question": question,
        "sql": sql,
        "columns": columns or "",
        "filters": filters or "",
        "num_retries_debug_sql": 0,
        "max_num_retries_debug": int(max_retries),
        "result_debug_sql": "",
        "error_msg_debug_sql": "",
        "df": pd.DataFrame(),
        "visualization_request": "",
        "python_code_data_visualization": "",
        "num_retries_debug_python_code_data_visualization": 0,
        "result_debug_python_code_data_visualization": "",
        "error_msg_debug_python_code_data_visualization": "",
        "python_code_store_variables_dict": {},
    }
    return app.invoke(initial)
