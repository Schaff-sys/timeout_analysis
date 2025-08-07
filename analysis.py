import pandas as pd 
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os 
from pathlib import Path

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")


if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Variables de entorno incompletas.")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

## Loading and adjusting data
# Read the entire table into a DataFrame
df = pd.read_sql_query(text("SELECT * FROM events_dates_merged_ordered2343;"), con=engine)

# Alter dataframe to include teamId from previous action in every row
df['team_id_last'] = df['team_id'].shift(1)

# Alter dataframe to include game_time_seconds from previous action in every row
df['game_time_seconds_last'] = df['game_time_seconds'].shift(1)

# List of teams for for loops 
teams = df['team_id'].dropna().unique().astype(int).tolist()









## Caluclations for general success rate 


# Calculating immediate success rate - until next attack - no. of success/no.of attacks - after timeout

# Isolate total number of events following exclusions
results = []
added_indices = set()

for idx, row in df[df['type'] == 'Exclusion'].iterrows():
    exclusion_team = row['team_id']
    block_indices = [idx]
    
    for i in range(idx + 1, len(df)):
        if df.loc[i, 'team_id'] == exclusion_team:
            block_indices.append(i)
        else:
            break
    
    # Add only rows not already added to avoid duplicates 
    for i in block_indices:
        if i not in added_indices:
            results.append(df.loc[i])
            added_indices.add(i)

exclusions = pd.DataFrame(results)

# Calculate the number of successful exclusions per team
def calculate_exclusion_success_rate(exclusions, team_id):
    team_exclusions = exclusions[exclusions['team_id'] == team_id]

    exclusion_goal = len(team_exclusions[team_exclusions['shot_isGoal'] == True])

    exclusions_number = team_exclusions['type'].value_counts().get('Exclusion', 0)

    exclusion_percentage = exclusion_goal/exclusions_number

    return exclusion_percentage

# Collect a dataframe with data for all the teams on their successful exclusion rate 
results = []
for team in teams:
    exclusion_success_rate = calculate_exclusion_success_rate(exclusions, team)

    results.append({
        'team_id': team,
        'exclusion_success_rate': exclusion_success_rate
    })

df_exclusions = pd.DataFrame(results)


# Calculating percentage of successful attacks generally
df_teams = []
exclusion_results = []

def general_shot_percentage(team_id, df_exclusions):
    
        df_team = df[df['team_id'] == team_id] #Filter main dataframe to include only data from the given team 

        exclusion_rate = df_exclusions[df_exclusions['team_id'] == team_id]['exclusion_success_rate'].iloc[0] #Exclusion rate taken from previously calculated database 

        total_successes = len(df_team[df_team['shot_isGoal'].fillna(False).astype(bool)]) + ((len(df_team[df_team['type']=='Exclusion'])) * exclusion_rate) # Total number of times an attack results in an exclusion or shot which is a goal (Exclusion is multiplied by rate to get probability of goal)

        total_attempts = len(df_team[df_team['team_id'] != df_team['team_id_last']]) # Total number of attacks (Values in dataframe adjusted to always show teamId as a value beneficial to the attacking team)

        success_rate = (total_successes/total_attempts) * 100

        rows = {
            'teamid': team_id,
            'success rate immediate': success_rate,
            'exclusion rate immediate': exclusion_rate,
            'total successes immediate': total_successes,
            'total attempts immediate': total_attempts
        }

        return rows

for team in teams:
    result = general_shot_percentage(team, df_exclusions)
    exclusion_results.append(result)

df_exclusion_results = pd.DataFrame(exclusion_results)




        
## Specific calculations for timeouts 
# Calculating immediate success rate - immediate impact of timeout on following attack 

def get_timeout_values_immediate(team):
        results = []
        all_blocks = []

            # Loop through each row where timeout_teamId is not null
        for idx, row in df[df['timeout_teamId'].notnull()].iterrows():
                timeout_team = row['timeout_teamId'] #Obtain values of teams calling timeouts
                
                # Start with the row where timeout happens
                block = [row]
                
                # Look forward through the DataFrame
                for i in range(idx + 1, len(df)):
                    if df.loc[i, 'team_id'] == timeout_team:
                        block.append(df.loc[i])
                    else:
                        break  # Stop if team_id changes

                # Convert block list to DataFrame and store
                all_blocks.extend(block)

    
        timeout_df = pd.DataFrame(all_blocks)

        team_timeout_df = timeout_df[timeout_df['team_id'] == team]  # Filter for the specific team

        timeouts_taken = team_timeout_df['timeout_teamId'].notnull().sum()

        successful_timeouts = team_timeout_df['shot_isGoal'].fillna(False).astype(bool).sum()

        timeout_leadingto_exclusion = len(team_timeout_df[team_timeout_df['type'] == 'Exclusion'])

        rows = {
             'team_id': team,
             'timeouts_taken': timeouts_taken,
             'successful_timeouts': successful_timeouts,
             'timeout_leadingto_exclusion': timeout_leadingto_exclusion,
        }
        results.append(rows)

        return(results)



timeout_results = []  # To collect timeout values for each team

for team in teams:  
    result = get_timeout_values_immediate(team)  # This returns a list with one dict inside
    timeout_results.extend(result)  # Extend since result is a list

# Optionally turn into a DataFrame
df_timeout_results = pd.DataFrame(timeout_results)


merged_df = pd.merge(df_exclusion_results, df_timeout_results, left_on='teamid', right_on='team_id', how='outer')












# Calculating 3 min success rate
def get_timeout_values_4mins(team, df_exclusions):
        results = []
        all_blocks = []

        for idx, row in df[df['timeout_teamId'].notnull() & (df['team_id'] == team)].iterrows():
                timeout_time = row['game_time_seconds'] #Obtain values of teams calling timeouts
                
                # Start with the row where timeout happens
                block = [row]
                
                # Look forward through the DataFrame
                for i in range(idx + 1, len(df)):
                    if timeout_time - df.loc[i, 'game_time_seconds'] <= 240 and df.loc[i, 'matchId'] == row['matchId']:
                        block.append(df.loc[i])
                    else:
                        break  # Stop if team_id changes

                # Convert block list to DataFrame and store
                all_blocks.extend(block)

        timeout_df_general = pd.DataFrame(all_blocks)
        timeout_df_general = timeout_df_general.drop_duplicates()
        timeout_df_general_team = timeout_df_general[timeout_df_general['team_id'] == team]  # Get the team_id from the first row

        exclusion_rate = df_exclusions[df_exclusions['team_id'] == team]['exclusion_success_rate'].iloc[0]  # Exclusion rate taken from previously calculated database
        print(exclusion_rate)
        total_successes = len(timeout_df_general_team[timeout_df_general_team['shot_isGoal'].fillna(False).astype(bool)]) + ((len(timeout_df_general_team[timeout_df_general_team['type']=='Exclusion'])) * exclusion_rate) # Total number of times an attack results in an exclusion or shot which is a goal (Exclusion is multiplied by rate to get probability of goal)
        print(total_successes)
        total_attempts = len(timeout_df_general_team[timeout_df_general_team['team_id'] != timeout_df_general_team['team_id_last']]) + (timeout_df_general_team['timeout_teamId']==team).sum() # Total number of attacks (Values in dataframe adjusted to always show teamId as a value beneficial to the attacking team)
        print(total_attempts)
        success_rate = (total_successes/total_attempts) * 100

        rows = {
            'teamid': team,
            'success rate 4mins': success_rate,
            'exclusion rate 4mins': exclusion_rate,
            'total successes 4mins': total_successes,
            'total attempts 4mins': total_attempts
        }

        results.append(rows)

        return(results)


timeout_results4 = []  # To collect timeout values for each team

for team in teams:  
    result4 = get_timeout_values_4mins(team, df_exclusions)  # This returns a list with one dict inside
    timeout_results4.extend(result4)  # Extend since result is a list

df_timeout_results4mins = pd.DataFrame(timeout_results4)


print(df_timeout_results4mins)