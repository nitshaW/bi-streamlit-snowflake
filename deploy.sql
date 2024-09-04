-- to be deployed as a Streamlit App with: snowsql -c demo_conn -f deploy.sql
-- CREATE OR REPLACE DATABASE transaction_streamlit;

-- CREATE STAGE mystage;
use schema SALES_ANALYTICS.PUBLIC;

create or replace stage bi_streamlit_stage;

PUT file:///Users/nitshawacinski/Desktop/bi-streamlit-snowflake/Main.py @bi_streamlit_stage overwrite=true auto_compress=false;
PUT file:///Users/nitshawacinski/Desktop/bi-streamlit-snowflake/data_store.py @bi_streamlit_stage overwrite=true auto_compress=false;
PUT file:///Users/nitshawacinski/Desktop/bi-streamlit-snowflake/pages/*.py @bi_streamlit_stage/pages overwrite=true auto_compress=false;

CREATE OR REPLACE STREAMLIT bi_analytics
    ROOT_LOCATION = '@SALES_ANALYTICS.public.bi_streamlit_stage'
    MAIN_FILE = '/Main.py'
    QUERY_WAREHOUSE = "STREAMLIT_XS";
