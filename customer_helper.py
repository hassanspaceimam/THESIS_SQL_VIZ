# customer_helper.py
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableMap

from config import get_llm

# ---------------- LLM ----------------
llm = get_llm()

# ===========================
# Subquestion selection
# ===========================
template_subquestion = ChatPromptTemplate.from_messages([
    ("system", """
You are an intelligent subquestion generator that extracts subquestions based on human instruction and the CONTEXT provided. You are part of a Text-to-SQL agent.

STRICT OUTPUT CONTRACT (read carefully):
- Return ONLY a JSON array (no backticks, no markdown, no extra text).
- Each element MUST be a 2-item array: ["<subquestion>", "<table_name>"].
- DO NOT group multiple subquestions into one element. If multiple subquestions map to the same table, emit multiple 2-item elements that reuse that table.
- Use double quotes for all strings.
- If no valid subquestions exist, return [] (an empty JSON array).

LINKING MINDSET:
- You may select a table even if it does not, by itself, answer a subquestion, provided it acts as a link to another table (e.g., order_id connects orders ↔ order_items ↔ order_payments). Prefer a single best table for each subquestion, but keep linking in mind.

DATASET-SPECIFIC MAPPING HINTS (very important):
- Seller performance (e.g., “Which seller received the most orders?”) → use the order_items table (it has seller_id) and count DISTINCT order_id per seller_id.
- Time trends for orders/sales → use orders.order_purchase_timestamp (e.g., monthly with DATE_FORMAT(..., '%Y-%m')).
- Total sales / revenue → SUM(order_payments.payment_value), joined to orders via order_id.
- Reviews → order_reviews.review_score linked by order_id.
- English category → category_translation.product_category_name_english joined to products.product_category_name.
"""),
    ("human", '''
CONTEXT:
This dataset pertains to Olist, the largest department store on Brazilian marketplaces.
When a customer purchases a product from the Olist store (from a specific seller and location), the seller is notified to fulfill the order.
Once the customer receives the product or the estimated delivery date passes, the customer receives a satisfaction survey via email to rate their purchase experience and leave comments.

You are given:
- A user question
- A list of table names with descriptions

Instructions:
Think like a Text-to-SQL agent. When selecting tables, carefully consider whether multiple tables need to be joined. Only select the tables necessary to answer the user question.
*** A table might not answer a subquestion, but adding it might act as a link with another table selected by a different agent. Think in this way while selecting a table. If one table has all the information, ignore others.

Your task:
1. Break the user question into minimal, specific subquestions that represent distinct parts of the information being requested.
2. For each subquestion, identify a **single best table** whose **description** clearly indicates it contains the needed information.
3. **Ignore any subquestion that cannot be answered using the provided tables.**
4. **Only include subquestions that directly contribute to answering the main user question.**
5. If a subquestion can be answered using multiple tables, choose the single most appropriate table based on the descriptions.
6. Be highly specific and avoid redundant or irrelevant subquestions (e.g., if the number of orders is asked, use order IDs; no extra details).

Output format (STRICT):
- Return ONLY valid JSON (no code fences, no prose).
- A JSON array of 2-item arrays: [["subquestion1","table1"], ["subquestion2","table2"], ...]
- DO NOT group multiple subquestions into a single element.
- If multiple subquestions map to the same table, repeat that table in separate elements.
- If no valid subquestions: []

Table List:
{tables}

User question:
{user_query}
''')
])

chain_subquestion = (
    RunnableMap({
        "tables": lambda x: x["tables"],
        "user_query": lambda x: x["user_query"]
    })
    | template_subquestion
    | llm
    | StrOutputParser()
)

# ===========================
# Column selection
# ===========================
template_column = ChatPromptTemplate.from_messages([
    ("system", """
You are an intelligent data column selector that chooses the most relevant columns from a list of available column descriptions to help answer a subquestion ONLY.
Your selections will be used by a SQL generation agent, so choose **only those columns** that will help write the correct SQL query for a subquestion based on main question.

Act like you're preparing the exact inputs required to build the SQL logic. Also, look at main user question before selecting columns.
BUT main PRIORITY IS TO SELECT columns for subquestion.

HOW TO THINK STEP BY STEP:
- For each subquestion mentioned in subquestion below, think if <column1> in Column list might help in answering the question based on column description below. If no, check if this column can be used to answer any part of main question below.
- There can be critical dependencies between columns (e.g., totals need identifiers like order_id; multi-row facts like installments/items must be combined).
- Include supporting columns that help define or group the main entity (e.g., order_id if the question asks for order-level info).
- Only after processing the subquestion completely, look at main question to see if it adds any more relevant columns.

RULES:
1. ALWAYS include any unique identifiers related to the entity being queried (e.g., order_id, product_id, customer_id).
2. NEVER select the customer_unique_id column — it must always be ignored.
3. When a value depends on multiple rows/parts, include all columns required to fully calculate or group that metric.
4. Output must be a list of pairs: [["<column name>", "<description and how it is used>"], ...] (each inner list length == 2).

LOCATION HINT (mandatory):
- If the question mentions *city*, *state*, or *location* for customers:
  - Select customer.customer_city and/or customer.customer_state.
  - Also include orders.customer_id so the query can join orders → customer.
- If the question mentions seller location:
  - Select sellers.seller_city and/or sellers.seller_state and include order_items.seller_id to join order_items → sellers.

Hints:
- For seller-level counts (e.g., “Which seller received the most orders?”), select order_items.seller_id and order_items.order_id (COUNT DISTINCT order_id per seller_id).
- For total sales/revenue, ensure order_payments.payment_value is selected; for time trends also include orders.order_purchase_timestamp.
"""),
    ("human", '''
Column list:
{columns}
     
subquestion:
{query}
     
Main question:
{main_question}
''')
])

chain_column_extractor = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"],
        "main_question": lambda x: x["main_question"]
    })
    | template_column
    | llm
    | StrOutputParser()
)

# ===========================
# Filter decision
# ===========================
template_filter_check = ChatPromptTemplate.from_messages([
    ("system", """
You help a text-to-SQL agent decide WHAT filters are implied by a user's question.
Return a STRICT JSON array:
- If no filter: ["no"]
- If filters exist: ["yes", ["<table>", "<column>", "<predicate>"], ...]
Where <predicate> is one of:
  - simple equality for categorical columns, e.g., "credit_card", "SP", "delivered"
  - numerical or date conditions, e.g., ">= 5", "< 100", "between 2017-01-01 and 2017-01-31", "after 2018-10-01", "before 2018-10-01"
Rules:
- Include only filters that truly narrow the dataset (e.g., city/state, payment_type, status, date ranges, numeric thresholds).
- If the question gives a natural language date like "last month", translate it into a relative predicate string, e.g., "last month" (the downstream agent will resolve it).
- Prefer equality for categorical values; use ranges for dates and numbers.

Return ONLY the JSON array, no prose.
"""),
    ("human", '''
User question:
{query}

Available tables and columns (with sample values):
{columns}
''')
])

chain_filter_extractor = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"]
    })
    | template_filter_check
    | llm
    | StrOutputParser()
)

# ===========================
# SQL generation
# ===========================
template_sql_query = ChatPromptTemplate.from_messages([
    ("system", """
You are an intelligent MySQL query generator.

STRICT OUTPUT CONTRACT
- Return ONLY a single MySQL query as plain text. No prose, no explanations, no markdown/code fences.
- The output MUST be a single syntactically valid statement. If you use a CTE, include the full WITH <name> AS (...) and close all parentheses.
- Do NOT include any leading commentary such as "Assuming...".
- Output must be ready to run as-is.

SCHEMA COMPLIANCE
- Use ONLY tables and columns that appear in "Relevant tables and columns" below (names exactly as given). Do not invent tables or columns.
- If a needed field is reachable via JOIN across the provided tables, join them using the identifiers described in those columns. Do not reference tables/columns outside the provided set.

KNOWN JOIN KEYS (helpful guidance, use only when present in the provided columns):
- orders.order_id ↔ order_items.order_id ↔ order_payments.order_id ↔ order_reviews.order_id
- orders.customer_id ↔ customer.customer_id
- order_items.product_id ↔ products.product_id
- products.product_category_name ↔ category_translation.product_category_name
- order_items.seller_id ↔ sellers.seller_id

SCHEMA MAPPING HINTS
- Customer city/state come from customer.customer_city / customer.customer_state via orders.customer_id = customer.customer_id. Do NOT use non-existent columns like orders.city or orders.state.

COLUMN USAGE POLICY
- All columns listed under "Relevant tables and columns" are mandatory for traceability. Use them in SELECT and/or in JOIN/WHERE/GROUP BY/HAVING as their descriptions imply.
- Do not include meaningless/unbound aliases or columns unrelated to the user question.

FILTERS
- Apply exactly the predicates given in "Applicable filters" below. Treat them literally (e.g., "between 2017-01-01 and 2017-01-31", ">= 5", "delivered").

AGGREGATION & DISTINCT
- When counting logical entities that can repeat across rows (e.g., multiple items per order), use COUNT(DISTINCT <entity_id>) as appropriate to match the user’s intent.
- For seller-level order counts from item-level data, compute COUNT(DISTINCT order_items.order_id) grouped by order_items.seller_id; order by that count DESC and limit as needed.

STYLE & SAFETY
- Use meaningful, short aliases (never SQL reserved words like 'or', 'and', 'as').
- Prefer CTEs for readability if the query is long/complex, but ensure the CTE is fully defined and referenced.
- For “average delivery time”, default to DAYS:
  use TIMESTAMPDIFF(DAY, orders.order_purchase_timestamp, orders.order_delivered_customer_date)
  and exclude NULL timestamps unless the user explicitly asks for hours.
- Ensure the final query is syntactically valid MySQL and optimized for correctness.

Return ONLY the final SQL statement.
"""),
    ("human", '''
User question:
{query}

Relevant tables and columns:
{columns}

Applicable filters:
{filters}
''')
])

chain_query_extractor = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"],
        "filters": lambda x: x["filters"]
    })
    | template_sql_query
    | llm
    | StrOutputParser()
)

# ===========================
# SQL validation
# ===========================
template_validation = ChatPromptTemplate.from_messages([
    ("system", """
You are a highly capable and precise MySQL query validator and fixer.

STRICT OUTPUT CONTRACT
- Return ONLY a single corrected MySQL query as plain text. No prose, no explanations, no markdown/code fences.
- If the provided query is fully correct, return it UNCHANGED (still SQL only).
- If a CTE alias is referenced (e.g., FROM base), ensure the CTE is fully declared with WITH <name> AS (...) and parentheses balanced. Fix dangling parentheses or undefined aliases.
- If the provided query has issues, return a revised SQL query that is syntactically valid and logically correct.

SCHEMA & INPUT COMPLIANCE
- The query must NOT reference any table or column that is not present in "Relevant Tables and Columns". If such a reference appears, rewrite the query to use only the provided tables/columns and their described relationships.
- If a location field is referenced on the wrong table (e.g., orders.city), replace it with customer.customer_city / customer.customer_state AND add the necessary join orders.customer_id = customer.customer_id, provided these columns are available in the inputs.
- Apply "Applicable filters" exactly as given (e.g., "between 2017-01-01 and 2017-01-31", ">= 5", "delivered"). Do not add or remove filters.

COLUMN & ALIAS POLICY
- Ensure required identifiers (join keys), grouping keys, and metric-related columns are present and used correctly.
- All selected columns are mandatory for traceability. Use them in SELECT and/or JOIN/WHERE/GROUP BY/HAVING according to their descriptions.
- Do not use SQL reserved words (e.g., 'or', 'and', 'as') as table or column aliases.

AGGREGATION & DISTINCT
- When counting logical entities that may repeat across rows (e.g., multiple items per order in order_items), prefer COUNT(DISTINCT <entity_id>) to match the intended entity-level count.
- For seller-level order counts derived from order_items, the query must use COUNT(DISTINCT order_id) grouped by seller_id; if not, rewrite it accordingly.

ROBUSTNESS
- For grouped results or counts with filters, use subqueries/CTEs where needed to avoid conflicts between GROUP BY, HAVING, and aggregates.
- For “average delivery time”, default to TIMESTAMPDIFF(DAY, order_purchase_timestamp, order_delivered_customer_date) with NULL filtering unless the user asked for hours.

Return ONLY the final SQL statement.
"""),
    ("human", '''
**User Question:**
{query}

**Relevant Tables and Columns:**
{columns}

**Applicable Filters:**
{filters}

**SQL Query to Validate:**
{sql_query}
''')
])

chain_query_validator = (
    RunnableMap({
        "columns": lambda x: x["columns"],
        "query": lambda x: x["query"],
        "filters": lambda x: x["filters"],
        "sql_query": lambda x: x["sql_query"],
    })
    | template_validation
    | llm
    | StrOutputParser()
)
