import time
import pandas as pd
import hashlib
import re
from datetime import datetime
import numpy as np
from date_functions import get_charge_time
import effekt_api
from tqdm import tqdm

db_fb_payments = effekt_api.get_all_fb_payments()

samlefil_df = pd.read_csv("samlefilFB.csv", skiprows=1)
samlefil_df['Charge Time PT'] = get_charge_time(samlefil_df)
samlefil_df = samlefil_df.loc[samlefil_df['Charge Time PT'] < '2022-01-01']

not_imported = []
incorrect_imports = []
imported_donations = []
for index, row in tqdm(samlefil_df.iterrows()):
    df_external_ID = str(row["ArkivID"])
    df_donation_sum = float(row["Sum NOK"].split()[0].replace(",",""))

    for i in range(len(db_fb_payments)):
        db_external_ID = str(db_fb_payments[i]['PaymentExternal_ID'])
        db_donation_sum = float(db_fb_payments[i]['sum_confirmed'])
        if (df_external_ID == db_external_ID):
            imported_donations.append(db_external_ID)
            if(df_donation_sum != db_donation_sum):
                incorrect_imports.append({
                    "db_external_ID": db_external_ID, 
                    "db_donation_sum": db_donation_sum, 
                    "df_donation_sum": df_donation_sum
                })
            continue

if(len(incorrect_imports) == 0):
    if(len(imported_donations) == len(samlefil_df)):
        print(f"All {len(imported_donations)} donations imported correctly")
else:
    print("Donations imported incorrectly: ")
    print(incorrect_imports)

# 1. Fetch all Facebook donations from DB using (sum, external payment ID and date)
# 2. Loop through all donations from samlefila
# 3. Check if donation sum in DB matches donation sum in samlefila
# 4. Check if both ms datetime and date fields for the donation in samlefila corresponds to the exact same date
