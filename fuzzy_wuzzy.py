# fuzzy_wuzzy.py
import re
import pandas as pd
from sqlalchemy import create_engine
from rapidfuzz import process, fuzz

from config import DB_URL

engine = create_engine(DB_URL)

def _get_values(table_name: str, column_name: str):
    query = f"SELECT DISTINCT {column_name} AS v FROM {table_name}"
    df = pd.read_sql(query, con=engine)
    return df["v"].dropna().astype(str).tolist()

def _best_fuzzy_match(input_value: str, choices):
    match, score, _ = process.extractOne(input_value, choices, scorer=fuzz.token_set_ratio)
    return match, score

def _flatten_filters_structure(filters):
    """
    Accept either:
      ["yes", ["t","c","v"], ["t","c","v"], ...]
    or the nested variant:
      ["yes", [ ["t","c","v"], ["t","c","v"], ... ]]
    and normalize to the flat version.
    """
    if (
        isinstance(filters, list)
        and filters
        and filters[0] == "yes"
        and len(filters) == 2
        and isinstance(filters[1], list)
        and filters[1]
        and all(isinstance(x, list) for x in filters[1])
    ):
        return ["yes", *filters[1]]
    return filters

def call_match(filters):
    """
    filters = ["yes", ["table","column","predicate"], ...]  or
              ["yes", [ ["table","column","predicate"], ... ]]

    For categorical equality-like predicates (no operators), fuzzy-match value to
    the column's distinct set. For others (numbers/dates/ranges), pass through unchanged.
    Returns the same ["yes", ...] with matched values.
    """
    if not isinstance(filters, list) or not filters or filters[0] == "no":
        return filters

    # NEW: normalize nested list form to flat form
    filters = _flatten_filters_structure(filters)

    out = ["yes"]
    for t in filters[1:]:
        if not isinstance(t, list) or len(t) < 3:
            # skip malformed entries rather than failing
            continue
        table, column, predicate = t[0], t[1], str(t[2]).strip()
        # Detect equality-like text (no operators/ranges/dates)
        if re.search(r"[A-Za-z]", predicate) and not re.search(
            r"\bbetween\b|<=|>=|<|>|before|after|\d{4}-\d{2}-\d{2}",
            predicate,
            re.I,
        ):
            choices = _get_values(table, column)
            best, _ = _best_fuzzy_match(predicate, choices) if choices else (predicate, 0)
            out.append([table, column, best])
        else:
            out.append([table, column, predicate])
    return out
