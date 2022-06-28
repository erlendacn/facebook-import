from datetime import datetime
import pytz

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