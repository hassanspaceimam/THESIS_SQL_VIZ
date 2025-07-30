# SQL & Visualization Agent (Streamlit)

Generate SQL from natural language, execute on MySQL, and render the best visualization automatically (Plotly) using LangChain/LangGraph + Azure OpenAI.

---

## Quickstart

```bash
# Create & activate a virtual environment
python -m venv .venv
# Windows (Git Bash)
source .venv/Scripts/activate
# Windows (PowerShell) alternative:
# .\.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env   # then edit .env with your real keys and DB URL
