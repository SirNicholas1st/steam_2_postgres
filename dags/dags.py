from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup as bs
import re

url = r"https://steamcommunity.com/app/730/reviews/?browsefilter=toprated&snr=1_5_100010_"
default_args = {
    "owner": "SirNicholas1st",
    "retries": 3,
    "retry_interval": timedelta(minutes=2)
}

@dag(
    dag_id = "Steam_review_to_postgres",
    start_date = datetime(2023, 5, 11),
    default_args = default_args,
    schedule = "@hourly"
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

        date = datetime.strptime(date, "%d %B, %Y")

        # assigning the variables to a dictionary
        res ={
            "found_helpful": found_helpful,
            "hours_on_record": hours_on_record,
            "date": date,
            "review_text": review_text,
            "poster": poster
        }

        return res

    task1 = PostgresOperator(
    task_id = "create_table",
    postgres_conn_id = "postgres_localhost",
    sql = "sql/review_table.sql"
    )

    task2 = extract_review(url)

    task3 = PostgresOperator(
        task_id = "delete_row_if_its_a_duplicate",
        postgres_conn_id = "postgres_localhost",
        sql = "sql/delete_row.sql"
    )

    task4 = PostgresOperator(
        task_id = "insert_row_to_table",
        postgres_conn_id = "postgres_localhost",
        sql = "sql/insert_row.sql"
    )

    task1 >> task2 >> task3 >> task4

steam_reviews_to_postgres()
