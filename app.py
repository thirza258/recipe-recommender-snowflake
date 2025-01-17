import streamlit as st
from snowflake.core import Root # requires snowflake>=0.8.0
from snowflake.snowpark.context import get_active_session
from datetime import datetime

def get_meal_time():
    # Get the current time
    now = datetime.now().time()

    # Define time periods for meals
    breakfast_start = datetime.strptime("06:00:00", "%H:%M:%S").time()
    lunch_start = datetime.strptime("12:00:00", "%H:%M:%S").time()
    dinner_start = datetime.strptime("18:00:00", "%H:%M:%S").time()
    night_start = datetime.strptime("21:00:00", "%H:%M:%S").time()

    # Determine the meal time
    if breakfast_start <= now < lunch_start:
        return "breakfast"
    elif lunch_start <= now < dinner_start:
        return "lunch"
    elif dinner_start <= now < night_start:
        return "dinner"
    else:
        return "late-night snack"

def get_time_of_day():
    now = datetime.now().time()

    morning_start = datetime.strptime("06:00:00", "%H:%M:%S").time()
    noon_start = datetime.strptime("12:00:00", "%H:%M:%S").time()
    afternoon_start = datetime.strptime("13:00:00", "%H:%M:%S").time()
    night_start = datetime.strptime("18:00:00", "%H:%M:%S").time()

    if morning_start <= now < noon_start:
        return "morning"
    elif noon_start <= now < afternoon_start:
        return "noon"
    elif afternoon_start <= now < night_start:
        return "afternoon"
    else:
        return "night"

def get_meal_time():
    now = datetime.now().time()

    breakfast_start = datetime.strptime("06:00:00", "%H:%M:%S").time()
    lunch_start = datetime.strptime("12:00:00", "%H:%M:%S").time()
    dinner_start = datetime.strptime("18:00:00", "%H:%M:%S").time()
    night_start = datetime.strptime("21:00:00", "%H:%M:%S").time()

    if breakfast_start <= now < lunch_start:
        return "breakfast"
    elif lunch_start <= now < dinner_start:
        return "lunch"
    elif dinner_start <= now < night_start:
        return "dinner"
    else:
        return "late-night snack"


def init_service_metadata():
    """
    Initialize the session state for cortex search service metadata. Query the available
    cortex search services from the Snowflake session and store their names and search
    columns in the session state.
    """
    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
        service_metadata = []
        if services:
            for s in services:
                svc_name = s["name"]
                svc_search_col = session.sql(
                    f"DESC CORTEX SEARCH SERVICE {svc_name};"
                ).collect()[0]["search_column"]
                service_metadata.append(
                    {"name": svc_name, "search_column": svc_search_col}
                )

        st.session_state.service_metadata = service_metadata

def query_cortex_search_service(query):
    db, schema = session.get_current_database(), session.get_current_schema()
    cortex_search_service = (
        root.databases[db]
        .schemas[schema]
        .cortex_search_services["RECIPE_DESCRIPTION_SERVICE"]
    )
    context_documents = cortex_search_service.search(
        query, columns=[], limit=5
    )
    results = context_documents.results
    service_metadata = st.session_state.service_metadata
    search_col = [s["search_column"] for s in service_metadata
                    if s["name"] == "RECIPE_DESCRIPTION_SERVICE"][0]
    
    list_rag = []
    
    for i, r in enumerate(results):
        chunk = r[search_col]
        
        # Query the original table using the chunk
        result = session.sql("""
            SELECT id, url, title, author, ingredients, steps
            FROM recipe_description_chunks
            WHERE chunk = ?
            """, [chunk]).collect()
        
        if not result:  # Skip if no matching row found
            continue
            
        row = result[0]
        context_str = ""
        
        # Try accessing the row as a named tuple
        try:
            # Get field names from the row's _fields attribute if it exists
            fields = row._fields
            for field in fields:
                value = getattr(row, field)
                context_str += f"{field}: {value}\n"
        except AttributeError:
            # If not a named tuple, try accessing by index
            # Use the order of columns from the SELECT statement
            columns = ['id', 'url', 'title', 'author', 'ingredients', 'steps']
            for idx, col in enumerate(columns):
                try:
                    value = row[idx]
                    context_str += f"{col}: {value}\n"
                except IndexError:
                    continue
        
        context_str += "\n"
        list_rag.append(context_str)
    
    return list_rag


def main():
    st.title(f"Recipe Recommender")

    init_service_metadata()

    status_time = get_time_of_day()
    status_lunch = get_meal_time()

    st.write(f"Your time right now is {status_time} then it is {status_lunch}")
    
    question = f"What is the right recipe for {status_time} or {status_lunch} time"
    result = query_cortex_search_service(question)
    headline = "Create a headline for reccomending the recipe for dinner time"
    headline_answer = session.sql("SELECT snowflake.cortex.complete(?,?)", ("mistral-large2", headline)).collect()[0][0]
    st.write(headline_answer) 
    
    for i in result:
        prompt = f"""
        paraphrasing the context below and keep the link in markdown format so 
        make it like the recipe for food like start with title and the author and the paraphrasing of the step
        and ingredients and end it with the url provided in context
        <context>
        {i}
        </context>
        <time>{status_time}</time>
        <status food>{status_lunch}</status food>
        """
        answer = session.sql("SELECT snowflake.cortex.complete(?,?)", ("mistral-large2", prompt)).collect()[0][0]
        st.markdown(answer)

    footer = """
    <style>
    .footer {
        left: 0;
        bottom: 0;
        width: 100%;
        text-align: center;
        padding: 10px;
    }
    </style>
    <div class="footer">
        <p>© 2025 Recipe Recommender. MIT License.</p>
    </div>
    """

    st.markdown(footer, unsafe_allow_html=True)

if __name__ == "__main__":
    session = get_active_session()
    root = Root(session)
    main()