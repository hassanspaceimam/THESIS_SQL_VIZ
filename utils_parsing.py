# utils_parsing.py
import ast
import json
import re
from typing import List

def parse_nested_list(text: str) -> list:
    """
    Parse a model output that should be a JSON/Python nested list.
    Tries JSON, then Python literal, then extracts the first top-level list from text.
    Always returns a list (possibly empty).
    """
    if not text:
        return []
    s = text.strip()
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, list) else []
    except Exception:
        pass
    try:
        obj = ast.literal_eval(s)
        return obj if isinstance(obj, list) else []
    except Exception:
        pass
    m = re.search(r"\[\s*\[.*?\]\s*(,\s*\[.*?\]\s*)*\]", s, re.DOTALL)
    if m:
        try:
            obj = ast.literal_eval(m.group(0))
            return obj if isinstance(obj, list) else []
        except Exception:
            return []
    return []

def normalize_subquestions(entries: list) -> List[List[str]]:
    """
    Ensure each entry is exactly [subquestion, table].
    """
    norm: List[List[str]] = []
    for e in entries:
        if not e:
            continue
        if isinstance(e, list) and len(e) >= 2:
            subq = str(e[0]).strip()
            table = str(e[1]).strip()
            if subq and table:
                norm.append([subq, table])
    return norm

def extract_sql(text: str) -> str:
    """
    Extract SQL either from a fenced ```sql ...``` block OR from the first SELECT onward.
    Falls back to returning stripped text if no SELECT found.
    """
    if not text:
        return ""
    s = str(text)

    # 1) Prefer an explicit ```sql ...``` fenced block (case-insensitive)
    m = re.search(r"```(?:\s*sql)?\s*(.*?)```", s, flags=re.I | re.S)
    if m:
        inner = m.group(1).strip()
        # If the fence had no 'sql' and included other code, still prefer the content.
        return inner

    # 2) Else take from the first SELECT onward
    m = re.search(r"(?is)\bselect\b.*", s, flags=re.I | re.S)
    if m:
        return m.group(0).strip()

    # 3) Fallback: raw text
    return s.strip()
