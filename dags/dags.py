from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup as bs
import re

url = r"https://steamcommunity.com/app/730/reviews/?browsefilter=mostrecent&snr=1_5_100010_&p=1"
default_args = {
    "owner": "SirNicholas1st",
    "retries": 3,
    "retry_interval": timedelta(minutes=2)
}

# Using the taskflow api
@dag(
    dag_id = "Steam_review_to_postgres_v2",
    start_date = datetime(2023, 5, 11),
    default_args = default_args,
    schedule = "*/30 * * * *",
    catchup = False
)
def steam_reviews_to_postgres():

    @task(multiple_outputs = True)
    def extract_review(url):
        
        page = requests.get(url)
        soup = bs(page.content, "html.parser")

        # getting the text from the div, this div contains found_helpful, hours_on_record, date & review_text + deleting tabs and carriage returns.
        review = soup.find("div", {"class": "apphub_UserReviewCardContent"}).get_text().strip().replace("\t", "").replace("\r", "")
        review = "\n".join(filter(None, review.split("\n")))

        # regex to extract info from the text
        # this will result in a list of numbers and we only want the "found helpful" number so we need to slice the list with [0] when using this regex
        pattern_helpful = r"\d+(?:,\d{3})*" 
        # also a list but with only one value, so slicing with [0] also when using this regex
        pattern_hours = r"([\d.]+)" 

        # extracting the found_helpful number, we will split the string and use regex pattern to find numbers from the first element and getting the first element of the matches.
        found_helpful = re.findall(pattern_helpful, review.split("\n")[0])[0].replace(",", "")
        # similar to previous, hours is on the third element when the string in splitted from line swaps.
        hours_on_record = re.findall(pattern_hours, review.split("\n")[2])[0]
        # no need for regex to get the date, we can just split the string and select the 2nd element.
        date = review.split("\n")[3].split("Posted: ")[1]
        # the review itself is on the 5th element
        review_text = review.split("\n")[4]

        # getting the text from the div, this div contains the poster. Stripping extra whitespace and splitting the string from line swap
        # and finally taking the first element from the list which contains the posters name.
        poster = soup.find("div", {"class": "apphub_friend_block_container"}).get_text().strip().split("\n")[0] 

        # Reviews posted during the current year wont have a year in their date.
        try:
            date = datetime.strptime(date, "%d %B, %Y")
        
        except:
            date_string = f"{date}, {datetime.today().year}"
            date = datetime.strptime(date_string, "%d %B, %Y")

        # Doubling the apostrophes, without this the sql will raise an error
        review_text = review_text.replace("'", "''")

        # assigning the variables to a dictionary
        res ={
            "found_helpful": found_helpful,
            "hours_on_record": hours_on_record,
            "date": date,
            "review_text": review_text,
            "poster": poster
        }

        return res

    # Formula for creating SQL queries for deleting and inserting rows.
    @task(multiple_outputs = True)
    def create_sql_queries(poster, found_helpful, hours_on_record, date, review_text):
        insert_statement = f"""
                            INSERT INTO Review_table (poster, found_helpful, hours_on_record, date, review_text)
                            VALUES ('{poster}', {found_helpful}, {hours_on_record}, '{date}', '{review_text}')"""
        
        delete_statement =f"""
                            DELETE FROM Review_table
                            WHERE poster = '{poster}' AND review_text = '{review_text}'"""
        
        return {"insert_statement": insert_statement,
                "delete_statement": delete_statement}


    # actual tasks start from this point onwards.
    # First task creates the table if it doesnt exist. Note, the connection to the database needs to be defined in Airflow.
    task1 = PostgresOperator(
    task_id = "create_table",
    postgres_conn_id = "postgres_localhost",
    sql = "sql/review_table.sql"
    )

    # 2nd task will fetch the review data from Steam as defined in the formula.
    task2 = extract_review(url)

    # 3rd task creates 2 the review specific SQL queries with the defined formula.
    task3 = create_sql_queries(poster= task2["poster"], found_helpful= task2["found_helpful"], hours_on_record= task2["hours_on_record"],
                               date= task2["date"], review_text= task2["review_text"])

    # 4th task deletes rows (row) from the table if there is already a row with the same poster and review text.
    task4 = PostgresOperator(
        task_id = "delete_row_if_its_a_duplicate",
        postgres_conn_id = "postgres_localhost",
        sql = task3["delete_statement"]
    )

    # The last task then adds the data to the table
    task5 = PostgresOperator(
        task_id = "insert_row_to_table",
        postgres_conn_id = "postgres_localhost",
        sql = task3["insert_statement"]
    )

    task1 >> task2 >> task3  >> task4 >> task5

steam_reviews_to_postgres()
