import pyodbc

import json


def table_exists(cursor, table_name):
    query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
    cursor.execute(query)
    return cursor.fetchone()[0] > 0
def dbconnect():
    server = '(LocalDB)\\MSSQLLocalDB'  # LocalDB instance name
    database = 'master'  # Database name (you can change this to your specific database)
    username = 'DUCENITCHN\\101730'  # Windows username in the format DOMAIN\\USERNAME
    password = ''  # Windows Authentication does not require a password

# Construct connection string for Windows Authentication with LocalDB
    connection_string = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};Trusted_Connection=yes'


# Connect to the database
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    json_file = 'Ml_project.individual_bat_stat.json'

# Read JSON data from file
    with open(json_file, 'r') as file:
        data = json.load(file)

    for json_data in data:
    
        if not table_exists(cursor, 'Batsman_Stats'):
            # # Construct SQL statement to create table
            create_table_sql = f'''
            CREATE TABLE Batsman_Stats (
                InningsPlayed INT,
                TotalRuns INT,
                TotalFours INT,
                TotalSixes INT,
                HighestRuns INT,
                Fiftys INT,
                Hundreds INT,
                Zeros INT,
                TotalBalls INT,
                Outs INT,
                NotOuts INT,
                TotalStrikeRate FLOAT,
                Average FLOAT,
                PlayerID NVARCHAR(255),
                PlayerName NVARCHAR(255),
                PlayerFullName NVARCHAR(255)
            )
            '''

            # Execute SQL statement to create table
            cursor.execute(create_table_sql)
            connection.commit()

            print(f"Table 'Batsman_Stats created successfully.")

        # Construct SQL statement to insert data into the table
        insert_sql = f'''
        INSERT INTO Batsman_Stats (InningsPlayed, TotalRuns, TotalFours, TotalSixes,
                                                                            HighestRuns, Fiftys, Hundreds, Zeros, TotalBalls, Outs, NotOuts,
                                                                            TotalStrikeRate, Average, PlayerID, PlayerName, PlayerFullName)
        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''

        # Extract values from team_stats and insert into the table
        values = (json_data['innings_Played'], json_data['total_runs'], json_data['total_fours'],
                json_data['total_sixes'], json_data['Highest'], json_data['50\'s'], json_data['100\'s'],
                json_data['0\'s'], json_data['total_balls'], json_data['total_outs'], json_data['total_notouts'],
                json_data['average_strike_rate'], json_data['average'], json_data['_id'], json_data['player_name'],
                json_data['player_full_name'])

        cursor.execute(insert_sql, values)
        connection.commit()

        print(f"Data inserted into table 'Batsman_Stats' successfully.")
    connection.commit()  # Commit the transaction
    print("Table created successfully.")
    connection.close()


if __name__=='__main__':
    dbconnect()