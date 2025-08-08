import pandas as pd 
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os 
from pathlib import Path
from scipy import stats 


# -------------------
# Environment Setup
# -------------------
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
df = pd.read_sql_query(text("SELECT * FROM events_dates_merged_ordered_withnames2343;"), con=engine)

# Alter dataframe to include teamId from previous action in every row
df['team_id_last'] = df['team_id'].shift(1)

# Alter dataframe to include game_time_seconds from previous action in every row
df['game_time_seconds_last'] = df['game_time_seconds'].shift(1)

# List of teams for for loops 
teams = df['team_id'].dropna().unique().astype(int).tolist()

teamnames = df[['team_id', 'team_name']]


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
            'team_id': team_id,
            'success rate general': success_rate
        }

        return rows

for team in teams:
    result = general_shot_percentage(team, df_exclusions)
    exclusion_results.append(result)

df_exclusion_results = pd.DataFrame(exclusion_results)




        
## Specific calculations for timeouts 
# Calculating immediate success rate - immediate impact of timeout on following attack 

def get_timeout_values_immediate(team, df_exclusions):
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

        exclusion_rate = df_exclusions[df_exclusions['team_id'] == team]['exclusion_success_rate'].iloc[0]

        timeouts_taken = team_timeout_df['timeout_teamId'].notnull().sum()

        successful_timeouts = team_timeout_df['shot_isGoal'].fillna(False).astype(bool).sum()

        timeout_leadingto_exclusion = len(team_timeout_df[team_timeout_df['type'] == 'Exclusion'])

        success_rate = (successful_timeouts + (timeout_leadingto_exclusion * exclusion_rate)) / timeouts_taken * 100 if timeouts_taken > 0 else 0

        rows = {
             'team_id': team,
             'success rate immediate': success_rate
        }
        results.append(rows)

        return(results)



timeout_results = []  # To collect timeout values for each team

for team in teams:  
    result = get_timeout_values_immediate(team, df_exclusions)  # This returns a list with one dict inside
    timeout_results.extend(result)  # Extend since result is a list

# Optionally turn into a DataFrame
df_timeout_results = pd.DataFrame(timeout_results)



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
        total_successes = len(timeout_df_general_team[timeout_df_general_team['shot_isGoal'].fillna(False).astype(bool)]) + ((len(timeout_df_general_team[timeout_df_general_team['type']=='Exclusion'])) * exclusion_rate) # Total number of times an attack results in an exclusion or shot which is a goal (Exclusion is multiplied by rate to get probability of goal)
        total_attempts = len(timeout_df_general_team[timeout_df_general_team['team_id'] != timeout_df_general_team['team_id_last']]) + (timeout_df_general_team['timeout_teamId']==team).sum() # Total number of attacks (Values in dataframe adjusted to always show teamId as a value beneficial to the attacking team
        success_rate = (total_successes/total_attempts) * 100

        rows = {
            'team_id': team,
            'success rate 4mins': success_rate
        }

        results.append(rows)

        return(results)


timeout_results4 = []  # To collect timeout values for each team

for team in teams:  
    result4 = get_timeout_values_4mins(team, df_exclusions)  # This returns a list with one dict inside
    timeout_results4.extend(result4)  # Extend since result is a list

df_timeout_results4mins = pd.DataFrame(timeout_results4)


# Calculating 2 min success rate
def get_timeout_values_2mins(team, df_exclusions):
        results = []
        all_blocks = []

        for idx, row in df[df['timeout_teamId'].notnull() & (df['team_id'] == team)].iterrows():
                timeout_time = row['game_time_seconds'] #Obtain values of teams calling timeouts
                
                # Start with the row where timeout happens
                block = [row]
                
                # Look forward through the DataFrame
                for i in range(idx + 1, len(df)):
                    if timeout_time - df.loc[i, 'game_time_seconds'] <= 120 and df.loc[i, 'matchId'] == row['matchId']:
                        block.append(df.loc[i])
                    else:
                        break  # Stop if team_id changes

                # Convert block list to DataFrame and store
                all_blocks.extend(block)

        timeout_df_general = pd.DataFrame(all_blocks)
        timeout_df_general = timeout_df_general.drop_duplicates()
        timeout_df_general_team = timeout_df_general[timeout_df_general['team_id'] == team]  # Get the team_id from the first row

        exclusion_rate = df_exclusions[df_exclusions['team_id'] == team]['exclusion_success_rate'].iloc[0]  # Exclusion rate taken from previously calculated database
        total_successes = len(timeout_df_general_team[timeout_df_general_team['shot_isGoal'].fillna(False).astype(bool)]) + ((len(timeout_df_general_team[timeout_df_general_team['type']=='Exclusion'])) * exclusion_rate) # Total number of times an attack results in an exclusion or shot which is a goal (Exclusion is multiplied by rate to get probability of goal)
        total_attempts = len(timeout_df_general_team[timeout_df_general_team['team_id'] != timeout_df_general_team['team_id_last']]) + (timeout_df_general_team['timeout_teamId']==team).sum() # Total number of attacks (Values in dataframe adjusted to always show teamId as a value beneficial to the attacking team)
        success_rate = (total_successes/total_attempts) * 100

        rows = {
            'team_id': team,
            'success rate 2mins': success_rate
        }

        results.append(rows)

        return(results)


timeout_results2 = []  # To collect timeout values for each team

for team in teams:  
    result2 = get_timeout_values_2mins(team, df_exclusions)  # This returns a list with one dict inside
    timeout_results2.extend(result2)  # Extend since result is a list

df_timeout_results2mins = pd.DataFrame(timeout_results2)


merged_df = pd.merge(
    df_exclusion_results,
    df_timeout_results2mins,
    left_on='team_id',
    right_on='team_id',
    how='outer'
)

merged_df = pd.merge(
    merged_df,
    df_timeout_results4mins,
    left_on='team_id',
    right_on='team_id',
    how='outer'
)

merged_df = pd.merge(
    merged_df,
    df_timeout_results,
    left_on='team_id',
    right_on='team_id',
    how='outer'
)

merged_df = pd.merge(
    merged_df,
    teamnames,
    left_on='team_id',
    right_on='team_id',
    how='outer'
)

merged_df.to_csv('timeout_analysis_results.csv', index=False)


# Calculate differences
merged_df["diff_2mins"] = merged_df["success rate 2mins"] - merged_df["success rate general"]
merged_df["diff_4mins"] = merged_df["success rate 4mins"] - merged_df["success rate general"]
merged_df["diff_immediate"] = merged_df["success rate immediate"] - merged_df["success rate general"]

# Paired t-tests
t_2, p_2 = stats.ttest_rel(merged_df["success rate 2mins"], merged_df["success rate general"])
t_4, p_4 = stats.ttest_rel(merged_df["success rate 4mins"], merged_df["success rate general"])
t_im, p_im = stats.ttest_rel(merged_df["success rate immediate"], merged_df["success rate general"])

# Wilcoxon signed-rank tests
w_2, wp_2 = stats.wilcoxon(merged_df["success rate 2mins"], merged_df["success rate general"])
w_4, wp_4 = stats.wilcoxon(merged_df["success rate 4mins"], merged_df["success rate general"])
w_im, wp_im = stats.wilcoxon(merged_df["success rate immediate"], merged_df["success rate general"])

# Pearson correlations
corr_2, cp_2 = stats.pearsonr(merged_df["success rate general"], merged_df["success rate 2mins"])
corr_4, cp_4 = stats.pearsonr(merged_df["success rate general"], merged_df["success rate 4mins"])
corr_im, cp_im = stats.pearsonr(merged_df["success rate general"], merged_df["success rate immediate"])

# Spearman correlations
scorr_2, sp_2 = stats.spearmanr(merged_df["success rate general"], merged_df["success rate 2mins"])
scorr_4, sp_4 = stats.spearmanr(merged_df["success rate general"], merged_df["success rate 4mins"])
scorr_im, sp_im = stats.spearmanr(merged_df["success rate general"], merged_df["success rate immediate"])

results = {
    "Paired t-test": {
        "2mins": (t_2, p_2),
        "4mins": (t_4, p_4),
        "immediate": (t_im, p_im)
    },
    "Wilcoxon": {
        "2mins": (w_2, wp_2),
        "4mins": (w_4, wp_4),
        "immediate": (w_im, wp_im)
    },
    "Pearson": {
        "2mins": (corr_2, cp_2),
        "4mins": (corr_4, cp_4),
        "immediate": (corr_im, cp_im)
    },
    "Spearman": {
        "2mins": (scorr_2, sp_2),
        "4mins": (scorr_4, sp_4),
        "immediate": (scorr_im, sp_im)
    }
}

clean_results = []
for test, vals in results.items():
    for condition, (stat, p) in vals.items():
        clean_results.append({
            "Test": test,
            "Condition": condition,
            "Statistic": round(float(stat), 4),
            "p-value": round(float(p), 6)
        })

results_df = pd.DataFrame(clean_results)
results_df.to_csv('timeout_analysis_statistics.csv', index=False)




