import pyodbc

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain.chains.sql_database.prompt import PROMPT_SUFFIX,_oracle_prompt
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import os
import re
import pandas as pd
from dotenv import load_dotenv


load_dotenv()
os.environ['GOOGLE_API_KEY']=os.getenv('GOOGLE_API_KEY')

def getdb(question):

    print("inside the method")
    server = '(LocalDB)\\MSSQLLocalDB'  # LocalDB instance name
    database = 'master'  # Database name (you can change this to your specific database)
    username = 'DUCENITCHN\\101730'  # Windows username in the format DOMAIN\\USERNAME
    password = ''  # Windows Authentication does not require a password

    # Construct connection string for Windows Authentication with LocalDB
    conn_str = 'mssql+pyodbc://@' + server + '/' + database + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    db=SQLDatabase.from_uri(conn_str)
   
    




    def get_schema(_):
        return db.get_table_info()
   

    def get_dialect(_):
        return db.dialect
    
   


    llm=ChatGoogleGenerativeAI(model='gemini-pro',temperature=0.1)


    oracle_prompt = """
    
     <Dialect>{dialect}</Dialect>
    
  
    You are an SQL expert. Given an input question, create a syntactically correct SQL query to run, answering the question with proper joins and subqueries based on the relationships of the tables provided and choose the Correct columns of the tables given to join tables
    Check the Column name of the tables from the schema
    Pay attention to dialact given  and generate Proper query based on dialect dont genrate other query
    Give only  the Query to run  Based  on the given Dailect only to run in respective database to fetch result and avoid duplicates in the result and give Proper result 
   **Instructions:**
    - Use appropriate alias naming conventions.
    - Avoid using reserved keywords like IS, AND, and AS as aliases, as they may cause syntax errors.
    - Pay attention to the dialect provided and generate a query specific to that dialect.
    -Understand  Relationship of the Tables based on question  and give the query
   
     Use the following Detail
    <SCHEMA>{schema}</SCHEMA>

    highest score means Highest in one game 
    most runs,total runs is Total runs
    Generate Complex Query carefully and check the Joins and subquery conditions Properly and Provide the result 
    Use the table Batsman_Stats with PlayerID as primary key to get the list of Players and their stas overall
    Use the table Batsman_Current_Stats  to get the stats of ofplayer for  the team they represented 
    Use the table Batsman_Opposition_Stats  to get the stats of of player for  the team they Played againts 
    Use the table Batsman_Innings_Stats  to get the stats of of player for  innings wise as 1st inning or second innings overall
    Use the table Batsman_Venue_Stats  to get the stats of of player for  at venue wise overall
    Use the table Batsman_Venue_inningswise_Stats   to get the stats of of player for  at venueat innings  wise overall
    Use the table Batsman_Season_Stats  to get the stats of of player for  each season  wise overall

    so use the tables name more accurate and column  based on question asked  and understand Relationship of the Tables 
    
     Use specific table names and columns based on the question asked and the relationships between the tables.

Example 1:
Question: "Player with Most sixes against a Team"
SQL:
WITH TeamMaxSixes AS (
    SELECT
        TeamPlayedAgaints,
        MAX(TotalSixes) AS MaxSixes
    FROM
        Batsman_Opposition_Stats
    GROUP BY
        TeamPlayedAgaints
),
TeamWiseSixes AS (
    SELECT
        BOS.TeamPlayedAgaints,
        BOS.PlayerID,
        BOS.TotalSixes
    FROM
        Batsman_Opposition_Stats BOS
    JOIN
        TeamMaxSixes TMS ON BOS.TeamPlayedAgaints = TMS.TeamPlayedAgaints
                       AND BOS.TotalSixes = TMS.MaxSixes
)
SELECT
    TWS.TeamPlayedAgaints,
    BS.PlayerID,
    BOS.TotalSixes
FROM
    TeamWiseSixes TWS
JOIN
    Batsman_Stats BS ON TWS.PlayerID = BS.PlayerID
JOIN
    Batsman_Opposition_Stats BOS ON TWS.TeamPlayedAgaints = BOS.TeamPlayedAgaints
                                   AND TWS.PlayerID = BOS.PlayerID
                                   AND TWS.TotalSixes = BOS.TotalSixes;

Example 2:
Question: "Top 5 players with Highest score"
SQL:
SELECT TOP 5
    PlayerName,
    MAX(HighestRuns) AS HighestScore
FROM
    Batsman_Current_Stats
GROUP BY
    PlayerName
ORDER BY
    HighestScore DESC;

Example 3:
Question: "Top 10 players with Most Sixes in Chepak Stadium"
SQL:

    SELECT TOP 5 
        BCTVS.PlayerName,
        BCTVS.TotalSixes
       
   
    from 
        Batsman_Current_team_venue_Stats  BCTVS
		
    WHERE
        BCTVS.Venue = 'MA Chidambaram Stadium, Chepauk, Chennai'

	 Order by TotalSixes DESC;
Example 4:
Question: "top 10 players with most runs in 1st innngs and 2nd innings seprately"
SQL:
WITH InningsStats AS (
    SELECT
        BS.PlayerName,
        BIS.Innings,
        SUM(BIS.TotalRuns) AS TotalRuns
    FROM
        Batsman_Innings_Stats BIS
    JOIN
        Batsman_Stats BS ON BIS.PlayerID = BS.PlayerID
    WHERE
        BIS.Innings IN ('1st_innings', '2nd_innings')
    GROUP BY
        BS.PlayerName,
        BIS.Innings
)

SELECT
    PlayerName,
    Innings,
    TotalRuns
FROM
    (
        SELECT
            PlayerName,
            Innings,
            TotalRuns,
            ROW_NUMBER() OVER (PARTITION BY Innings ORDER BY TotalRuns DESC) AS RowNum
        FROM
            InningsStats
    ) AS RankedStats
WHERE
    RowNum <= 10
ORDER BY
    Innings, TotalRuns DESC;

Question: {question}

SQL: "Your turn"
    """
    # question="top 5 run getters based on Venues played  in inningswise seprately"
    prompt = PromptTemplate.from_template(template=oracle_prompt)
  

    chain=(RunnablePassthrough.assign(dialect=get_dialect,schema=get_schema,)  | prompt |llm | StrOutputParser())


    response=chain.invoke({"question": question})
    
    sql_query_cleaned = response.strip().lstrip("```sql|'''mssql").rstrip("```").strip()
    columns=extract_column_names(sql_query_cleaned)
  
    print(columns)
    
    print(sql_query_cleaned)
    result=db.run(sql_query_cleaned)
    
   
    return result,columns


def extract_column_names(sql_query):
   
    sql_query = sql_query.strip()
    sql_query = sql_query.lower()

   
    top_clause = re.search(r'^SELECT\s+TOP\s+\d+', sql_query, re.IGNORECASE)
    if top_clause:
        sql_query = re.sub(r'^SELECT\s+TOP\s+\d+', 'SELECT', sql_query, flags=re.IGNORECASE)

    # Extract columns between SELECT and FROM keywords
    start_idx = sql_query.find('select') + len('select')
    end_idx = sql_query.find('from')
    columns_str = sql_query[start_idx:end_idx].strip()

    column_names = []
    for col in columns_str.split(','):
        col_parts = col.strip().split('.')
        if len(col_parts) > 1:
            col_name = col_parts[-1].replace('(', '').replace(')', '').split()[-1]  # Remove AS and its reference
        else:
            col_name = col_parts[0].replace('(', '').replace(')', '').split()[-1]  # Remove
        column_names.append(col_name)
    return column_names

   

     
    
# if __name__=='__main__':
   
#     response=getdb()
#     print(response)



    