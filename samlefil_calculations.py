from datetime import datetime
import pandas as pd
from date_functions import get_charge_time

samlefil_df = pd.read_csv("samlefilFB.csv", skiprows=1)
samlefil_df['Charge Time PT'] = get_charge_time(samlefil_df)
samlefil_df = samlefil_df.loc[samlefil_df['Charge Time PT'] < '2022-01-01']

donations_sum = 0
sum_yearly = {}
count = 0
for index, row in samlefil_df.iterrows():
    count += 1
    donations_sum += float(str(row['Sum NOK']).replace(",", "").split(" ")[0])
    
    year = row['Charge Time PT'].split("-")[0]
    year_from_date = row["Dato"].split("-")[0]

    if (year != year_from_date):
        print(f"Years not equal for donation {row['ArkivID']}")

    if (year in sum_yearly.keys()):
        sum_yearly[year] += float(str(row['Sum NOK']).replace(",", "").split(" ")[0])
    else:
        sum_yearly[year] = 0

print("Donations counted: " + str(count))
print("Donation sum: " + str(donations_sum))
print("Yearly sum:")
print(sum_yearly)