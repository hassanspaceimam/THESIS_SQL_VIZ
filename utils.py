# utils.py
import re

def extract_code_block(content: str, language: str) -> str:
    """
    Extract code from a fenced block:
      ```<language>
      ...
      ```
    If not found, return the whole content minus backticks.
    """
    if content is None:
        return ""
    s = str(content)

    # 1) Look for ```language ...``` (case-insensitive on language)
    pattern_lang = rf"```(?:\s*{re.escape(language)})\s*(.*?)```"
    m = re.search(pattern_lang, s, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # 2) Fallback: first generic fenced block ``` ... ```
    m = re.search(r"```(.*?)```", s, re.DOTALL)
    if m:
        return m.group(1).strip()

    # 3) Last resort: return content without stray backticks
    return s.replace("```", "").strip()
