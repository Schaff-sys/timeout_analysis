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
            'success rate': success_rate,
            'exclusion_rate': exclusion_rate,
            'total_successes': total_successes,
            'total_attempts': total_attempts
        }

        return rows

for team in teams:
    result = general_shot_percentage(team, df_exclusions)
    exclusion_results.append(result)

df_exclusion_results = pd.DataFrame(exclusion_results)

print(df_exclusion_results)


        
## Specific calculations for timeouts 
# Calculating immediate success rate - immediate impact of timeout on following attack 

def get_timeout_values(team):
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
    result = get_timeout_values(team)  # This returns a list with one dict inside
    timeout_results.extend(result)  # Extend since result is a list

# Optionally turn into a DataFrame
df_timeout_results = pd.DataFrame(timeout_results)


merged_df = pd.merge(df_exclusion_results, df_timeout_results, left_on='teamid', right_on='team_id', how='outer')

print(df_timeout_results)

print(merged_df)












# Calculating 3 min success rate

results = []

# Loop through each row where timeout_teamId is not null
for idx, row in df[df['timeout_teamId'].notnull()].iterrows():
    timeout_team = row['timeout_teamId']
    
    # Start with the row where timeout happens
    block = [row]
    
    # Look forward through the DataFrame
    for i in range(idx + 1, len(df)):
        if df.loc[i, 'game_time_seconds'] - df.loc[idx, 'game_time_seconds'] <= 180 and df.loc[i, 'team_id'] == timeout_team and df.loc[i, 'matchId'] == df.loc[idx,'matchId']:
            block.append(df.loc[i])
        else:
            break  # Stop if team_id changes

    # Convert block list to DataFrame and store
    block_df = pd.DataFrame(block)
    results.append(block_df)

final_result = pd.concat(results).reset_index(drop=True)

timeouts_taken = len(final_result['timeout_teamId'].notnull())

successful_timeouts = len(final_result[(final_result['shot_isGoal']) | (final_result['type'] == 'Exclusion')])

success_rate_after_timeout = (successful_timeouts / timeouts_taken) * 100 if timeouts_taken > 0 else 0


# Calculating 5 min success rate 


results = []

# Loop through each row where timeout_teamId is not null
for idx, row in df[df['timeout_teamId'].notnull()].iterrows():
    timeout_team = row['timeout_teamId']
    
    # Start with the row where timeout happens
    block = [row]
    
    # Look forward through the DataFrame
    for i in range(idx + 1, len(df)):
        if df.loc[i, 'game_time_seconds'] - df.loc[idx, 'game_time_seconds'] <= 300 and df.loc[i, 'team_id'] == timeout_team:
            block.append(df.loc[i])
        else:
            break  # Stop if team_id changes

    # Convert block list to DataFrame and store
    block_df = pd.DataFrame(block)
    results.append(block_df)

final_result = pd.concat(results).reset_index(drop=True)

timeouts_taken = len(final_result['timeout_teamId'].notnull())

successful_timeouts = len(final_result[(final_result['shot_isGoal']) | (final_result['type'] == 'Exclusion')])

success_rate_after_timeout = (successful_timeouts / timeouts_taken) * 100 if timeouts_taken > 0 else 0



