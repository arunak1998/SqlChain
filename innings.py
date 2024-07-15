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
    json_file = 'Ml_project.batsman_current_team_venue_stat.json'

# Read JSON data from file
    with open(json_file, 'r') as file:
        data = json.load(file)

    for json_data in data:
        for item in json_data:
            # Extract team name and its stats
            if item=='_id':
                continue
            team_name = item
            venues_played =json_data[team_name]
            for venues in venues_played:
                venue=venues
                venue_stat=venues_played[venue]
                innings_played =venue_stat.get('innings_played', [])
                for inning in innings_played:
                    innings=next(iter(inning.keys()))
                    inning_details = next(iter(inning.values()))

            
                    if not table_exists(cursor, 'Batsman_Current_team_venue_inningwise_Stats'):
                # Construct SQL statement to create table
                        create_table_sql = f'''
                        CREATE TABLE Batsman_Current_team_venue_inningwise_Stats (
                            TeamPlayedfor NVARCHAR(255),
                            Venue NVARCHAR(255),
                            Innings NVARCHAR(255),
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

                    # # Execute SQL statement to create table
                        cursor.execute(create_table_sql)
                        connection.commit()

                        print(f"Table 'Batsman_Current_team_venue_inningwise_Stats created successfully.")

                    # Construct SQL statement to insert data into the table
                    insert_sql = f'''
                    INSERT INTO Batsman_Current_team_venue_inningwise_Stats (TeamPlayedfor,Venue,Innings, InningsPlayed, TotalRuns, TotalFours, TotalSixes,
                                                                                        HighestRuns, Fiftys, Hundreds, Zeros, TotalBalls, Outs, NotOuts,
                                                                                        TotalStrikeRate, Average, PlayerID, PlayerName, PlayerFullName)
                    VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''

                    # Extract values from team_stats and insert into the table
                    values = (team_name,venue,innings, inning_details['innings_Played'], inning_details['total_runs'], inning_details['total_fours'],
                            inning_details['total_sixes'], inning_details['highest_runs'], inning_details['fiftys'], inning_details['hundreds'],
                            inning_details['zeros'], inning_details['total_balls'], inning_details['outs'], inning_details['not_outs'],
                            inning_details['total_strike_rate'], inning_details['average'], inning_details['_id'], inning_details['player_name'],
                            inning_details['player_full_name'])

                    cursor.execute(insert_sql, values)
                    connection.commit()

            print(f"Data inserted into table 'Batsman_Current_team_venue_inningwise_Stats' successfully.")
        connection.commit()  # Commit the transaction
    print("Table created successfully.")
    connection.close()


if __name__=='__main__':
    dbconnect()