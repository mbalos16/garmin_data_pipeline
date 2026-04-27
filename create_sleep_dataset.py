# Imports
import json
import pandas as pd
import glob
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "DI_CONNECT", "DI-Connect-Wellness")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "results","final_datasets")

COLUMNS_MAPPER_SLEEP = {
    'sleepStartTimestampGMT': "sleep_start",
    'sleepEndTimestampGMT': "sleep_end",
    'calendarDate': "date",
    'sleepWindowConfirmationType': "confirmation_type",
    'deepSleepSeconds': "deep_sleep_sec",
    'lightSleepSeconds' : "light_sleep_sec",
    'remSleepSeconds': "rem_sleep_sec",
    'awakeSleepSeconds': "awake_sleep_sec",
    'unmeasurableSeconds': "unmeasurable_sec",
    'averageRespiration': "average_respirations",
    'lowestRespiration': "lowest_respiration",
    'highestRespiration': "highest_respiration",
    'awakeCount' : "awake_count",
    'avgSleepStress': "sleep_stress_avg",
    'restlessMomentCount': "restless_count",
    'sleepScores.overallScore' : "overall_score",
    'sleepScores.qualityScore' : "quality_score",
    'sleepScores.durationScore': "duration_score",
    'sleepScores.recoveryScore': "recovery_score",
    'sleepScores.deepScore': "deep_sleep_score",
    'sleepScores.remScore': "rem_sleep_score",
    'sleepScores.lightScore': "light_sleep_score",
    'sleepScores.awakeningsCountScore': "awakenings_score",
    'sleepScores.awakeTimeScore': "awake_time_score",
    'sleepScores.combinedAwakeScore': "combined_awake_score",
    'sleepScores.restfulnessScore': "restfulness_score",
    'sleepScores.interruptionsScore': "interruption_score",
    'sleepScores.feedback': "feedback",
    'sleepScores.insight': "insight"
}

COLUMNS_TO_DROP_SLEEP = ["retro", "confirmation_type"]
PERCENTAGE_COLS_SLEEP = ['light_sleep_hours', 'rem_sleep_hours', 'awake_sleep_hours', 'unmeasurable_hours', "deep_sleep_hours"]


def load(data_dir:str, data_name: str) -> pd.DataFrame:
    '''
    Searches all the files from the data_dir where the name of the file in .json extension that matches data_name.
    Reads those JSON files, flatten and normalize them.
    '''
    files_path = glob.glob(os.path.join(data_dir, f"{data_name}.json"))
    if not files_path:                                                                                                                                        
        raise FileNotFoundError(f"No files named: '{data_name}.json' found in {data_dir}")
    
    data = []

    for path in files_path:
        path_size = os.path.getsize(path)
        if path_size > 0:
            with open(path, "r", encoding='utf-8') as f:                                                                                                                                 
                content = json.load(f)
            data.append(content)

    # Flat the data
    flat_data = [item for sublist in data if isinstance(sublist, list) for item in sublist]
    df = pd.json_normalize(flat_data)
    df = df.sort_values("calendarDate").reset_index(drop=True)
    return df

def clean_data(df:pd.DataFrame, columns_mapper:dict, drop_cols:list, sec_hours: bool) -> pd.DataFrame:
    '''
    Gets all the data from inside the dataframe and cleans it:
    - rename the columns based on columns_mapper.
    - drops the columns given in drop_cols.
    - converts the columns that have _sec at the end of the name in hours and renames the columns to match the new data.

    ''' 
    if columns_mapper:
        df.rename(columns = columns_mapper, inplace = True)
    if drop_cols:
        df.drop(columns = drop_cols, inplace = True)
    if sec_hours:
        sec_to_hours = {col: col.replace("_sec", "_hours") for col in df.columns if col.endswith("_sec")}
        df[list(sec_to_hours)] = df[list(sec_to_hours)] / 3600                                           
        df.rename(columns=sec_to_hours, inplace=True)  
    if "rem_sleep_hours" in df:
        # Substitute the values allocated inside rem_sleep_hours with 0
        values = {"rem_sleep_hours": 0}
        df = df.fillna(value = values)
    return df

                                                                                    
def engineer_features_sleep(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Handle some data processes to create total_sleep_hours, sleep_efficiency, feedback_positive, and percentages.
    '''

    # Create new columns
    df['feedback_positive'] = df['feedback'].str.startswith('POSITIVE').fillna(False)
    df["total_sleep_hours"] = (df["deep_sleep_hours"] + df["light_sleep_hours"] + df["rem_sleep_hours"])
    total_sleep = df["total_sleep_hours"] + df["awake_sleep_hours"]
    # dataframe["col"].div(dataframe["col"]) returns NANs when division by 0 which are next filled with 0.
    df["sleep_efficiency"] = df["total_sleep_hours"].div(total_sleep).fillna(0)

    # Datetime transformation
    df["sleep_start"] = pd.to_datetime(df["sleep_start"])                                                                                         
    df["sleep_end"]   = pd.to_datetime(df["sleep_end"])  
    df["date"]        = pd.to_datetime(df["date"])

    # Add percentages of sleep
    for column in PERCENTAGE_COLS_SLEEP:
        column_name = f"{column}_percentage"
        df[column_name] = (df[column] / df['total_sleep_hours'])* 100
    return df


def export(df: pd.DataFrame, output_path:str) -> None:
    '''
    Export the dataset to the location stored inside output_path.
    '''
    os.makedirs(output_path, exist_ok=True)
    df.to_csv(os.path.join(output_path, "sleep_data.csv"), index=False) 

def main():
    df = load(data_dir=DATA_DIR, data_name="*_sleepData")                                                                                                                       
    df = clean_data(
        df = df, 
        columns_mapper=COLUMNS_MAPPER_SLEEP, 
        drop_cols=COLUMNS_TO_DROP_SLEEP, 
        sec_hours=True)
    df = engineer_features_sleep(df)                                                                                                                            
    export(df, OUTPUT_PATH)

if __name__=="__main__":
    main()