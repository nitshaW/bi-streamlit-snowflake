# Create a Multi-Page Dashboard with as a Streamlit App

- **Main.py** - multi-page Python entry code, to deploy as a Streamlit App in Snowflake.
- **pages/\*.py** - Python code for the multi-page Streamlit App, one page per chart type.
- **deploy.sql** - SQL script to deploy as a Streamlit App in Snowflake.

## Actions

Deploy the last multi-page version as a Streamlit App, running **`snowsql -c demo_conn -f deploy.sql`**. Check that there are no errors (i.e. no text in red on screen). Test the app in the Snowflake web UI.
