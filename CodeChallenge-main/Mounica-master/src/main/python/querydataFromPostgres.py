from src.main.python.dbConnection import *


table = "Covid_Data_Analysis"
conn_type="regular"

conn, curr = db_connection_open(conn_type)

try:
    print("Total row count")
    curr.execute(f'select count(*) from {table}')
    row = curr.fetchone()
    while row:
        print(row)
        row = curr.fetchone()

    print("Total cases and deaths in USA")
    curr.execute(f'select sum(new_case),sum(new_death) from {table}')
    row = curr.fetchone()
    while row:
        print(row)
        row = curr.fetchone()

    print("Top 5 states with high COVID Cases")
    curr.execute(f'select state, max(total_cases) as cnt from {table} group by state order by cnt desc limit 6')
    row = curr.fetchone()
    while row:
        print(row)
        row = curr.fetchone()

    print("Total cases and death in California as of 5/20/2021")
    state="'CA'"
    curr.execute(f"select sum(new_case) , sum(new_death) from {table} where state={state} and submission_date <= '2021-05-20'   ")
    row = curr.fetchone()
    while row:
        print(row)
        row = curr.fetchone()

    print("When percent increase of Covid cases is more than 20% compared to previous submission in California")
    state = "'CA'"
    curr.execute(
        f"Select submission_date, total_cases from (select submission_date, state, total_cases , lag(total_cases) over (partition by state order by submission_date) as previous_submission, total_cases*.2 as twenty_percent from {table} where state={state} order by submission_date ) as a where total_cases-previous_submission > twenty_percent")
    row = curr.fetchone()
    while row:
        print(row)
        row = curr.fetchone()

    print("safest state comparatively")
    state = "'CA'"
    curr.execute(
        f"select state, max(total_cases) as cnt from {table} group by state order by cnt limit 1")
    row = curr.fetchone()
    while row:
        print(row)
        row = curr.fetchone()

except p.Error as e:
    print("Error while sending request for queriyng songs")
    print(e)


db_connection_close(conn_type,conn,curr)