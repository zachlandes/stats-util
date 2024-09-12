import duckdb
from contextlib import closing
import csv
from io import StringIO
import praw
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import matplotlib.dates as mdates

SCRIPT_VERSION = "1.0.0"

# Load environment variables
load_dotenv()

# Set up Reddit connection
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=f"python:stats-util:{SCRIPT_VERSION} (by u/{os.getenv('REDDIT_USERNAME')})",
    username=os.getenv("REDDIT_USERNAME"),
    password=os.getenv("REDDIT_PASSWORD")
)

DEBUG = False  # Set this to False to exclude test subreddits
EXCLUDED_SUBREDDITS = ['FundraisersOnReddit', 'fundraisersTests', 'SnoowyDayFund', 'cedartest', 'axolotl_playground']
FUNDRAISERS_APP_USER_ID = "14u2cffx3h"

def main():
    with closing(duckdb.connect('fundraisers.db')) as con:
        # Create table if it doesn't exist
        con.execute("""
            CREATE TABLE IF NOT EXISTS fundraisers (
                PostID VARCHAR,
                FundraiserID VARCHAR,
                Raised INTEGER,
                Timestamp TIMESTAMP,
                Subreddit VARCHAR
            )
        """)

        # Create table to store last processed message
        con.execute("""
            CREATE TABLE IF NOT EXISTS last_processed (
                subreddit VARCHAR PRIMARY KEY,
                last_processed_id VARCHAR
            )
        """)

        # Retrieve last processed message ID
        last_processed_id = con.execute("""
            SELECT last_processed_id 
            FROM last_processed 
            WHERE subreddit = ?
        """, ["SnoowyDayFund"]).fetchone()

        last_processed_id = last_processed_id[0] if last_processed_id else None

        # Fetch messages from SnoowyDayFund subreddit
        subreddit = reddit.subreddit("SnoowyDayFund")
        new_last_processed_id = None
        for message in subreddit.modmail.conversations(limit=None, sort="recent", state="mod"):
            if message.authors[0].id != FUNDRAISERS_APP_USER_ID:
                continue
            if message.id == last_processed_id:
                break
            if new_last_processed_id is None:
                new_last_processed_id = message.id
            if message.subject.startswith("Daily Fundraiser Summary"):
                try:
                    subreddit_name = message.subject.split(": r/")[1]
                except IndexError:
                    print(f"Warning: Unexpected subject format: {message.subject}")
                    continue  # Skip this message

                content_lines = message.messages[0].body_markdown.strip().split('\n')
                
                # Skip header line, empty lines, and lines starting with "Subreddit:"
                data_lines = [line for line in content_lines if line.strip() and not line.startswith("PostID") and not line.startswith("Subreddit:")]
                
                # Prepare data for insertion
                data_to_insert = []
                for line in data_lines:
                    try:
                        post_id, fundraiser_id, raised, timestamp = line.split(',')
                        raised_int = int(raised)
                        timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                        data_to_insert.append((post_id, fundraiser_id, raised_int, timestamp_obj, subreddit_name))
                    except ValueError as e:
                        print(f"Warning: Error processing line '{line}': {str(e)}")
                        continue  # Skip this line
                
                # Insert data using VALUES clause
                if data_to_insert:
                    placeholders = ', '.join(['(?, ?, ?, ?, ?)' for _ in data_to_insert])
                    con.execute(f"""
                        INSERT INTO fundraisers (PostID, FundraiserID, Raised, Timestamp, Subreddit)
                        VALUES {placeholders}
                    """, [item for sublist in data_to_insert for item in sublist])

        # Update the last processed message ID after the loop
        if new_last_processed_id:
            con.execute("""
                INSERT OR REPLACE INTO last_processed (subreddit, last_processed_id)
                VALUES (?, ?)
            """, ["SnoowyDayFund", new_last_processed_id])

def calculate_top_fundraisers(con, limit=10):
    query = """
    WITH latest_daily_fundraiser AS (
        SELECT FundraiserID, Subreddit, Raised,
               ROW_NUMBER() OVER (PARTITION BY FundraiserID, DATE_TRUNC('day', Timestamp) ORDER BY Timestamp DESC) as rn
        FROM fundraisers
        WHERE ? OR Subreddit NOT IN (SELECT unnest(?))
    )
    SELECT FundraiserID, MAX(Raised) / 100.0 as MaxRaised, Subreddit
    FROM latest_daily_fundraiser
    WHERE rn = 1
    GROUP BY FundraiserID, Subreddit
    ORDER BY MaxRaised DESC
    LIMIT ?
    """
    results = con.execute(query, [DEBUG, EXCLUDED_SUBREDDITS, limit]).fetchall()
    print(f"\nTop {limit} Fundraisers:")
    for rank, (fundraiser_id, raised, subreddit) in enumerate(results, 1):
        print(f"{rank}. r/{subreddit} - {fundraiser_id}: ${raised:.2f}")

def create_daily_totals_chart(con):
    query = """
    WITH latest_daily_fundraiser AS (
        SELECT FundraiserID, Raised, DATE_TRUNC('day', Timestamp) as Date,
               ROW_NUMBER() OVER (PARTITION BY FundraiserID, DATE_TRUNC('day', Timestamp) ORDER BY Timestamp DESC) as rn
        FROM fundraisers
        WHERE ? OR Subreddit NOT IN (SELECT unnest(?))
    )
    SELECT Date, SUM(Raised) / 100.0 as DailyTotal
    FROM latest_daily_fundraiser
    WHERE rn = 1
    GROUP BY Date
    ORDER BY Date
    """
    results = con.execute(query, [DEBUG, EXCLUDED_SUBREDDITS]).fetchall()
    
    print(f"Number of reports: {len(results)}")  # Debugging line
    
    if not results:
        print("No data returned from the query.")
        return
    
    dates, totals = zip(*results)
    
    print("Sample of dates:", dates[:5])  # Debugging line
    print("Sample of totals:", totals[:5])  # Debugging line
    
    plt.figure(figsize=(12, 6))
    plt.plot(dates, totals, marker='o')
    plt.title("Daily Fundraising Totals")
    plt.xlabel("Date")
    plt.ylabel("Total Raised")
    
    # Format x-axis to show dates properly
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gcf().autofmt_xdate()  # Rotation
    
    # Format y-axis as dollar amounts
    def dollar_formatter(x, p):
        return f'${x:,.0f}'
    
    plt.gca().yaxis.set_major_formatter(FuncFormatter(dollar_formatter))
    
    plt.tight_layout()
    plt.savefig("daily_totals.png")
    plt.close()
    
    print("\nDaily totals chart saved as 'daily_totals.png'")

def create_subreddit_bar_chart(con):
    query = """
    WITH latest_fundraiser AS (
        SELECT FundraiserID, Subreddit, Raised,
               ROW_NUMBER() OVER (PARTITION BY FundraiserID ORDER BY Timestamp DESC) as rn
        FROM fundraisers
        WHERE ? OR Subreddit NOT IN (SELECT unnest(?))
    )
    SELECT Subreddit, SUM(Raised) / 100.0 as TotalRaised
    FROM latest_fundraiser
    WHERE rn = 1
    GROUP BY Subreddit
    ORDER BY TotalRaised DESC
    """
    results = con.execute(query, [DEBUG, EXCLUDED_SUBREDDITS]).fetchall()
    
    if not results:
        print("No data returned from the query.")
        return
    
    subreddits, totals = zip(*results)
    
    plt.figure(figsize=(12, 6))
    bars = plt.bar(subreddits, totals)
    plt.title("Total Raised by Subreddit")
    plt.xlabel("Subreddit")
    plt.ylabel("Total Raised ($)")
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    # Format y-axis as dollar amounts
    def dollar_formatter(x, p):
        return f'${x:,.0f}'
    
    plt.gca().yaxis.set_major_formatter(FuncFormatter(dollar_formatter))
    
    # Add value labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                 f'${height:,.2f}',
                 ha='center', va='bottom', rotation=0)
    
    plt.tight_layout()
    plt.savefig("subreddit_totals.png")
    plt.close()
    
    print("\nSubreddit totals chart saved as 'subreddit_totals.png'")

def calculate_subreddit_growth(con):
    query = """
    WITH latest_fundraiser AS (
        SELECT FundraiserID, Subreddit, Raised, Timestamp,
               ROW_NUMBER() OVER (PARTITION BY FundraiserID ORDER BY Timestamp DESC) as rn
        FROM fundraisers
        WHERE ? OR Subreddit NOT IN (SELECT unnest(?))
    ),
    subreddit_stats AS (
        SELECT lf.Subreddit, 
               MIN(DATE_TRUNC('day', Timestamp)) as FirstDay,
               MAX(DATE_TRUNC('day', Timestamp)) as LastDay,
               COUNT(DISTINCT DATE_TRUNC('day', Timestamp)) as DaysActive,
               COUNT(DISTINCT lf.FundraiserID) as TotalFundraisers,
               SUM(CASE WHEN lf.rn = 1 THEN lf.Raised ELSE 0 END) / 100.0 as TotalRaised
        FROM latest_fundraiser lf
        GROUP BY lf.Subreddit
    )
    SELECT *,
           CASE WHEN DaysActive > 0 THEN TotalRaised / DaysActive ELSE 0 END as AvgRaisedPerDay
    FROM subreddit_stats
    ORDER BY TotalRaised DESC
    """
    results = con.execute(query, [DEBUG, EXCLUDED_SUBREDDITS]).fetchall()
    return results

def print_subreddit_growth_and_performance(results):
    print("\nSubreddit Growth and Performance:")
    for subreddit, first_day, last_day, days_active, total_fundraisers, total_raised, avg_raised_per_day in results:
        print(f"r/{subreddit}:")
        print(f"  First fundraiser: {first_day}")
        print(f"  Last fundraiser: {last_day}")
        print(f"  Days with reports: {days_active}")
        print(f"  Total fundraisers: {total_fundraisers}")
        print(f"  Total raised: ${total_raised:.2f}")
        print(f"  Average raised per day: ${avg_raised_per_day:.2f}")
        print()

def print_all_rows(con, limit=None):
    query = """
    SELECT PostID, FundraiserID, Raised / 100.0 as RaisedDollars, Timestamp, Subreddit
    FROM fundraisers
    WHERE ? OR Subreddit NOT IN (SELECT unnest(?))
    ORDER BY Timestamp DESC
    """
    if limit is not None:
        query += f" LIMIT {limit}"
    
    results = con.execute(query, [DEBUG, EXCLUDED_SUBREDDITS]).fetchall()
    
    print("\nAll Fundraiser Entries:")
    print("PostID | FundraiserID | Raised ($) | Timestamp | Subreddit")
    print("-" * 80)
    for row in results:
        post_id, fundraiser_id, raised, timestamp, subreddit = row
        print(f"{post_id} | {fundraiser_id} | ${raised:.2f} | {timestamp} | r/{subreddit}")

def run_stats_suite(con):
    print("\n--- Running Stats Suite ---")
    calculate_top_fundraisers(con)
    create_daily_totals_chart(con)
    create_subreddit_bar_chart(con)
    results = calculate_subreddit_growth(con)
    print_subreddit_growth_and_performance(results)
    print_all_rows(con, limit=10)
    print("\n--- Stats Suite Complete ---")

if __name__ == "__main__":
    main()
    
    with closing(duckdb.connect('fundraisers.db')) as con:
        run_stats_suite(con)