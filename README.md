##This is built in a venv using Powershell and wsl
1. python3 -m venv dbt-env
2. source dbt-env/bin/activate
2. pip install dbt-snowflake
3. pip install dbt-core

1. Create a data warehouse.
2. Create database in warehouse.
3. Create role and provide permissions/grants.
4. Assign user the role to have permission in the warehouse.
5. Activate the role to start using.
6. Create a schema for the database.

1. Initialize dbt with '''dbt init'''
2. Name project: data_pipeline
3. For account, go to our snowflake webpage/Account under Admin 
on the left side menu, and copy the link next to the locator tag.
4. Provide the username from snowflake, create a password, provide
the role,the warehouse, schema names.
5. For threads select a number for the project. ex. 10

1. Open project in IDE
2. Open dbt_project.yml file and add the following under models:
Under example have the following:
    First change example to staging
example:
  +materalized: view
  snowflake_warehouse: <db warehouse name, ex. "dbt_wh">
marts: 
  +materalized: table
  snowflake_warehouse: <db warehouse name, ex. "db"
3. Delete example folder in data_pipeline/models
4. Create folder "staging" and "marts"
5. Next install 3rd party libraries, useful for surgate keys.
6. Create file packages.yml
7. Install package by running 'dbt deps'

1. Create a tpch_sources.yml file under models
2. Create stg_tpch_orders.sql
3. In terminal, type 'dbt run'

1. Fill in stg_tpch_orders.sql with:
```
select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from
    {{ source('tpch', 'orders') }}
```
2. Create file stg_tpch_line_items.sql. Fill with:
```
select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
    l_orderkey as order_key,
    l_partkey as part_key,
    l_linenumber as line_number,
    l_quantity as quantity,
    l_extendedprice as extended_price,
    l_discount as discount_percentage,
    l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}
```
3. In terminal, run: "dbt run -s stg_tcph_line_items"
4. Create new file int_order_items.sql under marts:
```
select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_items') }} as line_item
on orders.order_key = line_item.order_key
order by
    orders.order_date
```
4. Rerun "dbt run -s stg_tpch_orders", 
then run: "dbt run -s int_order_items"

## Create Macros
// Macros are a good way of reusing business logic across multiple models.
1. Create under marcos folder, pricing.sql:
The macros "discounted_amount" can be used a function in queries.
```
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```
Add:
	{{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount
to int_order_items.sql 
then run dbt run -s int_order_times" to update the columns.
2. Under marts, create int_order_items_summary.sql:
```
select
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key
```
3. Create fct_orders.sql:
```
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('int_order_items_summary') }} as order_item_summary
on orders.order_key = order_item_summary.order_key
order by order_date
```
4. dbt run

## Generic and Singular Tests
1. Create under marts file generic_tests.yml
```
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```
2. run: dbt test
3. Create under tests, fct_orders_discount.sql:
```
select
    *
from
    {{ ref('fct_orders')}}
where
    item_discount_mount > 0
```
4. run: dbt test
5. Create under tests, fact_orders_date_valid.sql:
```
select
    *
from
    {{ ref('fct_orders')}}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')
```
6. run: dbt test

Deploy models using Airflow
1. pip install astro
2. mkdir dbt-dag
3. astro dev init
4. Add the following to dbt-dag/Dockerfile:
```
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
pip install --no-cache-dir dbt-snowflake && deactivate
```
5. Add the following to dbt-dag/requirements.txt:
```
astronomer-cosmos
apache-airflow-providers-snowflake
```
6. Type astro dev start
7. Go to webpage: localhost:8080/
8. username and password are "admin"
9. Copy data_pipeline folder to dbt_dag/dags/
10. Create file dbt_dag.py
```
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name='default',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_conn',
        profile_args={'database':'dbt_db', 'schema':'dbt_schema'},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig('/dbt-dag/dags/data_pipeline',),
    operator_args={'install_deps': True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval='@daily',
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id='dbt_dag',
)
```
