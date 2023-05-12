!mamba install pandas==1.3.3 -y
!mamba install requests==2.26.0 -y
import glob
import pandas as pd
from datetime import datetime
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%

def extract_from_json(file_to_process):
    dataframe=pd.read_json(file_to_process,lines=True)
    return dataframe

def extract():
    #Create an empty dataframe to hold the extracted data
    extracted_data=pd.DataFrame(columns=['Name','Market Cap (US$ Billion)'])
    for jsonfile in glob.glob("*.json"):
        extracted_data=extracted_data.append(extract_from_json(jsonfile),ignore_index=True)
    return extracted_data
                                                         
def transform(dataframe):
    dataframe["Market Cap (GBP$ Billion)"]=dataframe["Market Cap (US$ Billion)"]*exchange_rate
    dataframe["Market Cap (GBP$ Billion)"]=dataframe["Market Cap (GBP$ Billion)"].round(3)
    return dataframe
                                                          
def load(targetfile,dataframe):
    dataframe.to_csv(targetfile,index=False)
                     
def log(message):
    timestamp=datetime.now().strftime("%Y-%h-%d-%H:%M:%S")
    print(f"[{timestamp}] {message}")

log("ETL Job Started")
log("Extract phase Started")                           
df1=extract_from_json("bank_market_cap_1.json")
print(df1.head())
log("Extract phase Ended")

log("Transform phase Started")
df=pd.read_csv("exchange_rates.csv",index_col=0)
exchange_rate=df.loc["GBP","Rate"]                              
transformed_data=transform(df1)                                                      
print(transformed_data.head())
log("Transform phase Ended")
                              
log("Load phase Started")
targetfile="bank_market_cap_gbp.csv"
load(targetfile,transformed_data)                              
log("Load phase Ended")
