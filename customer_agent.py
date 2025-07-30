# customer_agent.py
import os
import pickle
import re
from typing import Dict, Any, TypedDict, Annotated
from operator import add

from langgraph.graph import StateGraph, START, END

from customer_helper import chain_subquestion, chain_column_extractor
from utils_parsing import parse_nested_list, normalize_subquestions

# Load knowledgebase (try CWD first, then module dir for robustness)
_KB_FILENAME = "knowledgebase.pkl"
try:
    with open(_KB_FILENAME, 'rb') as f:
        loaded_dict = pickle.load(f)
except FileNotFoundError:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base_dir, _KB_FILENAME), 'rb') as f:
        loaded_dict = pickle.load(f)

# Table groups for router → tables
d_store = {
    "customer": ['customer', 'sellers'],
    "orders": ['order_items', 'order_payments', 'order_reviews', 'orders'],
    "product": ["products", "category_translation"],
}

class overallstate(TypedDict):
    user_query: str
    table_lst: list[str]
    table_extract: Annotated[list[str], add]
    column_extract: Annotated[list[str], add]

def agent_subquestion(q: str, v: str) -> str:
    response = chain_subquestion.invoke({"tables": v, "user_query": q}).replace("\n", "")
    # Return raw; parsing happens downstream
    return response

def solve_subquestion(q: str, lst: list[str]) -> str:
    final = []
    for tab in lst:
        desc = loaded_dict[tab][0]
        final.append([tab, desc])
    result_dict = {item[0]: item[1] for item in final}
    return agent_subquestion(q, str(result_dict))

def sq_node(state: overallstate):
    q = state['user_query']
    lst = state['table_lst']
    raw = solve_subquestion(q, lst) or "[]"
    parsed = parse_nested_list(raw)
    return {"table_extract": normalize_subquestions(parsed)}

def agent_column_selection(mq: str, q: str, c: str) -> str:
    response = chain_column_extractor.invoke({
        "columns": c, "query": q, "main_question": mq
    }).replace("\n", "")
    match = re.search(r"\[\s*\[.*?\]\s*(,\s*\[.*?\]\s*)*\]", response, re.DOTALL)
    return match.group(0) if match else "[]"

def solve_column_selection(main_q: str, list_sub: list[list[str]]) -> list[list[str]]:
    from utils_parsing import parse_nested_list  # keep local to mirror original
    final_col: list[list[str]] = []
    for tab in list_sub:
        if not tab:
            continue
        table_name = tab[-1]                           # robust: last is table
        question = " | ".join(tab[:-1]) or ""          # handles grouped or single
        columns = loaded_dict[table_name][1]
        out_column = agent_column_selection(main_q, question, str(columns))
        trans_col = parse_nested_list(out_column)      # safe parsing
        for col_selec in trans_col:
            if not isinstance(col_selec, list) or len(col_selec) < 2:
                continue
            final_col.append([f"name of table:{table_name}", *col_selec])
    return final_col

def column_node(state: overallstate):
    subq = state['table_extract']
    mq = state['user_query']
    o = solve_column_selection(mq, subq)
    return {"column_extract": o}

builder_final = StateGraph(overallstate)
builder_final.add_node("subquestion", sq_node)
builder_final.add_node("column_e", column_node)
builder_final.add_edge(START, "subquestion")
builder_final.add_edge("subquestion", "column_e")
builder_final.add_edge("column_e", END)
graph_final = builder_final.compile()
