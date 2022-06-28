import time
import pandas as pd
import hashlib
import re
from datetime import datetime
import numpy as np
import requests
import pytz 
import pymysql
import random
import json

# Organization names and IDs
organizations = {
    "Effekt": 11,
    "AMF": 1,
    "DtW": 5,
    "End": 3,
    "GD": 7,
    "GiveWell": 12,
    "HKI": 10,
    "MC": 4,
    "PHC": 8,
    "SCI": 2,
    "Sight": 6,
    "UBI": 13,
    "NI": 14
    }

def create_distribution_request(donorID, distribution, metaOwnerId, authToken=""):
    headers = {'Content-Type': 'application/json', "Authorization": authToken}
    payload = {
            "donor": {"id": donorID},
            "distribution": distribution,
            "metaOwnerId": metaOwnerId
        }

    r = requests.post(
        'http://localhost:3000/distributions/', 
        data=json.dumps(payload),
        headers=headers
    )

    return json.loads(r.content.decode('UTF-8'))["content"]

def get_KID_by_distribution_request(donorId, distribution):
    headers = {'Content-Type': 'application/json'}
    payload = {
            "donorId": donorId,
            "distribution": distribution
        }

    r = requests.post(
        'http://localhost:3000/distributions/KID/distribution', 
        data=json.dumps(payload),
        headers=headers
    )

    return r.content.decode('UTF-8')

def get_donorID_by_email_or_name(name, email):
    headers = {'Content-Type': 'application/json'}
    payload = {
            "name": name,
            "email": email
        }

    r = requests.post(
        'http://localhost:3000/donors/id/email/name', 
        data=json.dumps(payload),
        headers=headers
    )

    return json.loads(r.content.decode('UTF-8'))

def register_donation_request(sum, timestamp, KID, paymentId, paymentExternalRef, metaOwnerID):
    headers = {'Content-Type': 'application/json'}
    payload = {
        "sum": sum,
        "timestamp": timestamp,
        "KID": KID,
        "paymentId": paymentId,
        "paymentExternalRef": paymentExternalRef,
        "metaOwnerID": metaOwnerID
    }

    r = requests.post(
        'http://localhost:3000/donations/confirm', 
        data=json.dumps(payload),
        headers=headers
    )

    return r.content.decode('UTF-8')

def register_donor_request(name, email):
    headers = {'Content-Type': 'application/json'}
    payload = {
        "name": name,
        "email": email,
        "ssn": ""
    }

    r = requests.post(
        'http://localhost:3000/donors/', 
        data=json.dumps(payload),
        headers=headers
    )

    return r.content.decode('UTF-8')

def get_charge_time(df):
    """
    Function to change chargetime from US Pacific time to Oslo timezone.
    """
    charge_time = []

    for i in df.index:
        timestamp = df['Charge Time'][i]
        timestring = str(df['Charge Time PT'][i])

        if (timestamp == timestamp):
        
            d = datetime.fromtimestamp(int(timestamp))
            d = d.astimezone(pytz.timezone('Europe/Oslo'))
            charge_time.append(d.strftime("%Y-%m-%d %H:%M:%S"))

        else:
            d = datetime.strptime(timestring, "%Y-%m-%d %H:%M:%S")
            # Set the time zone to 'Us/Pacific'
            d = pytz.timezone('Us/Pacific').localize(d)
            # Transform the time to 'Europe/Oslo
            d = d.astimezone(pytz.timezone('Europe/Oslo'))
            charge_time.append(d.strftime("%Y-%m-%d %H:%M:%S"))
    return charge_time

def get_new_donors_from_fb(donors_from_db , donations_from_fb):
    """
    Finds the new donors from facebook donations, who have never donated before. Returns in the format required in the database
    """
    emails_list = []
    charge_time_list = []
    fullname = []
    meta_owner = []

    fullnames = donations_from_fb["Fornavn"] + ' ' + donations_from_fb["Etternavn"]

    index_email = donations_from_fb['E-post (FB eller DB)'].dropna().drop_duplicates().index

    # Finds the new donors with email registered
    for i in index_email:

        if (donations_from_fb['E-post (FB eller DB)'][i] not in donors_from_db["email"].values):

                emails_list.append(donations_from_fb['E-post (FB eller DB)'][i])
                charge_time_list.append(donations_from_fb['Charge Time PT'][i])
                #print(donations_from_fb['Charge Time PT'][i])
                if(fullnames[i] == fullnames[i]):
                    fullname.append(fullnames[i].title())
                else:
                    fullname.append('Missing name') # adds the name "Missing name" if donation don't have name attached to donation

                if (donations_from_fb['Charge Time PT'][i] < '2019-08-26'):
                    meta_owner.append(1)
                else:
                    meta_owner.append(3)

    index_fullnames = fullnames.drop_duplicates().index

    # Finds the new donors when email is not registered
    for i in index_fullnames:

        if ( donations_from_fb['E-post (FB eller DB)'][i] != donations_from_fb['E-post (FB eller DB)'][i] ): # donor has not registered email
        
            if ( str(fullnames[i]).lower() not in map(lambda x: x.lower(),donors_from_db["full_name"])):
                
                emails_list.append(f'donasjon+{str(fullnames[i]).lower().replace(" ", "_")}@gieffektivt.no')
                charge_time_list.append(donations_from_fb['Charge Time PT'][i])
                #print('Ikke epost', donations_from_fb['Charge Time PT'][i])
                if(fullnames[i] == fullnames[i]):
                    fullname.append(fullnames[i].title())
                else:
                    fullname.append('Missing name') # adds the name "Missing name" if donation don't have name attached to donation

                if (donations_from_fb['Charge Time PT'][i] < '2019-08-26'):
                    meta_owner.append(1)
                else:
                    meta_owner.append(3)

    # Adds the new donors to the format required in the database
    new_donors = donations_from_fb.copy()
    new_donors = new_donors.loc[new_donors.loc[donations_from_fb['Charge Time'].isin( charge_time_list )]['Charge Time'].drop_duplicates().index]
    new_donors["email"] = emails_list
    new_donors["full_name"] = fullname
    new_donors["date_registered"] = charge_time_list
    new_donors["password_hash"] = np.nan
    new_donors["password_salt"] = np.nan
    new_donors["ssn"] = np.nan
    new_donors["Meta_owner_ID"] = meta_owner
    new_donors["newsletter"] = np.nan
    new_donors["trash"] = np.nan

    print(f"new_donors {len(new_donors)}")

    # Returns the new donors in the format to fir with the database
    return new_donors[['email' , "full_name" , "date_registered" , "password_hash" , "ssn" , "Meta_owner_ID" , "newsletter" , "trash"]]

actual_sum_orgs = {}
distribution_sum_orgs = {}

def create_donation_object(row):
    meta_owner = row["Meta_owner_ID"]
    external_ref = row["ArkivID"]

    # Check if donor exists in DB by name or email
    first_name = row["Fornavn"]
    surname = row["Etternavn"]

    email = row["E-post (FB eller DB)"]

    # Is this necessary?
    if not (isinstance(email, str)):
        email = ""
    
    if not (isinstance(first_name, str)):
        first_name = ""

    if not (isinstance(surname, str)):
        surname = ""
    
    full_name = (first_name + " " + surname).strip().title()
    if (full_name == ""):
        full_name = "Missing name FB"

    
    # Override emails for known names
    if (full_name == "Aurora Normann"):
        email = "aurora.sund.strabo@gmail.com"

    res = get_donorID_by_email_or_name(full_name, email)
    donorID = res["content"]

    if not (isinstance(donorID, int)):
        email = f'donasjon+{str(full_name).lower().replace(" ", "_")}@gieffektivt.no'
        register_donor_request(full_name, email)
    
        # Fetch newly created donor
        res = get_donorID_by_email_or_name(full_name, email)
        donorID = res["content"]

    # Extract distribution from Samlefila
    donation_sum = float(row["Sum NOK"].split()[0].replace(",",""))
    distribution = []
    dist_sum = 0

    for abbriv, id in organizations.items():
        split_nok = float(str(row[abbriv]).split()[0].replace(",",""))
        split = split_nok/donation_sum*100

        if (float(split_nok) > 0):
            distribution.append({
                "organizationId": id,
                "share": str(round(split, 2))
                })

            # Calculate actual sum of donations for each organization
            dist_sum += split_nok
            if abbriv in actual_sum_orgs.keys():
                actual_sum_orgs[abbriv] += split_nok
            else:
                actual_sum_orgs[abbriv] = 0
    
    # Check if distribution sum is correct
    #if (dist_sum != donation_sum):
        #print(f"Donation {external_ref}: Distribution sum {dist_sum} different than actual sum {donation_sum}")
    
    # Check if distribution share adds to 100%
    dist_total_percent = 0
    for org in distribution:
        dist_total_percent += float(org["share"])
    if (dist_total_percent != 100):
        # Adjusts share of first organization to create a 100% total
        distribution[0]["share"] = str(round(float(distribution[0]["share"]) + float(100-dist_total_percent), 2))

    # Calculate sum for each organization based on distribution percentages
    for org in distribution:
        for abbriv, id in organizations.items():
            if (str(org["organizationId"]) == str(id)):
                if (abbriv in distribution_sum_orgs.keys()):
                    distribution_sum_orgs[abbriv] += round(donation_sum*(float(org["share"])/100), 1)
                else:
                    distribution_sum_orgs[abbriv] = 0

    # Retry in case of request failure due to network issues (move this to inside the request function)
    max_retries = 5
    retry_count = 0

    while retry_count <= max_retries:
        # Check if identical distribution already exists, create new if not
        kid = create_distribution_request(donorID, distribution, meta_owner, "")

        if (isinstance(kid, str)):
            break
        else:
            retry_count += 1
            time.sleep(1)
            if (retry_count > max_retries):
                print(f"Failed to create KID for donorID {donorID}")

    donation = {
        "sum": donation_sum,
        "timestamp": row["Charge Time PT"],
        "KID": kid,
        "paymentId": 9,
        "paymentExternalRef": external_ref,
        "Meta_owner_ID": meta_owner
    }

    retry_count = 0

    while retry_count <= max_retries:
        res = register_donation_request(
            donation["sum"], 
            donation["timestamp"], 
            donation["KID"], 
            9, 
            donation["paymentExternalRef"],
            donation["Meta_owner_ID"]
        )
        
        status = str(json.loads(res)["status"])
        if(status == "200"):
            break
        else:
            retry_count += 1
            time.sleep(1)
            if (retry_count > max_retries):
                # Wait in case slow network is causing the issue
                print(f"Failed to import donation {donation['paymentExternalRef']}")
                print(distribution)
                print(res)

from queue import Queue
from threading import Thread

class Worker(Thread):
  """ Thread executing tasks from a given tasks queue """

  def __init__(self, tasks):
    Thread.__init__(self)
    self.tasks = tasks
    self.daemon = True
    self.start()

  def run(self):
    while True:
      func, args, kargs = self.tasks.get()
      try:
        func(*args, **kargs)
      except Exception as e:
        print(e)
        # An exception happened in this thread
        pass
      finally:
        # Mark this task as done, whether an exception happened or not
        self.tasks.task_done()

class ThreadPool:
  """ Pool of threads consuming tasks from a queue """

  def __init__(self, num_threads):
    self.tasks = Queue(num_threads)
    for _ in range(num_threads):
      Worker(self.tasks)

  def add_task(self, func, *args, **kargs):
    """ Add a task to the queue """
    self.tasks.put((func, args, kargs))

  def map(self, func, args_list):
    """ Add a list of tasks to the queue """
    for args in args_list:
      self.add_task(func, args)

  def wait_completion(self):
    """ Wait for completion of all the tasks in the queue """
    self.tasks.join()

def create_donation_objects(samlefil_df):

    iter_count = 0
    starting_iteration = 0
    max_iterations = 9000

    pool = ThreadPool(8)

    start = datetime.now()
    # Iterate through all donations in samlefila
    for index, row in samlefil_df.iterrows():

        iter_count += 1
        if(iter_count%250 == 0 and iter_count >= starting_iteration):
            now = datetime.now()
            elapsed = now - start
            print(f"Iteration {iter_count} | {round((elapsed.seconds/60), 2)} minutes")
        if iter_count < starting_iteration:
            continue
        if iter_count > max_iterations:
            break

        pool.add_task(create_donation_object, row)

    pool.wait_completion()
        
    print("Actual sum orgs:")
    print(actual_sum_orgs)
    print("Distribution sum orgs:")
    print(distribution_sum_orgs)

pd.set_option('display.max_columns', 500)
samlefil_df = pd.read_csv("samlefilFB.csv", skiprows=1) # gets the csv-file from excel with donations from facebook
samlefil_df['Charge Time PT'] = get_charge_time(samlefil_df)
samlefil_df = samlefil_df.loc[samlefil_df['Charge Time PT'] < '2022-01-01 00:00:00']
samlefil_df['Meta_owner_ID'] = (samlefil_df['Charge Time PT'] < '2019-08-26 00:00:00').apply(lambda x: 1 if x==True else 3)

#donors_db = pd.read_csv("Donors_from db.csv", encoding = "ISO-8859-1", sep=";")
#new_donors = get_new_donors_from_fb(donors_db , samlefil_df)

print("Running")
create_donation_objects(samlefil_df)
print("All donations imported")

donations_sum = 0
for index, row in samlefil_df.iterrows():
    donations_sum += float(str(row['Sum NOK']).replace(",", "").split(" ")[0])
print(f"Total donations imported: {len(samlefil_df)}")
print("Expected donation sum in DB: " + str(donations_sum))