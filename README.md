# SQL & Visualization Agent (Streamlit)

Generate SQL from natural language, execute on MySQL, and render the best visualization automatically (Plotly) using LangChain/LangGraph + Azure OpenAI.

## Quickstart
```bash
python -m venv .venv
source .venv/Scripts/activate    # Windows Git Bash
pip install -r requirements.txt
cp .env.example .env             # then edit .env with your real keys and DB URL

# If knowledgebase.pkl isn't committed, build it:
python build_knowledgebase.py

# Run the app
streamlit run streamlit_chat.py


