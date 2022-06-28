import requests
import json

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

def get_all_fb_payments(authToken=""):
    headers = {"Authorization": authToken}

    r = requests.get(
        'http://localhost:3000/facebook/payments/all',
        headers=headers
    )

    return json.loads(r.content.decode('UTF-8'))["content"]

def get_donation_by_external_ID(externalID, methodID, authToken=""):
    headers = {"Authorization": authToken}

    r = requests.get(
        f'http://localhost:3000/donations/externalID/{externalID}/{methodID}',
        headers=headers
    )

    return json.loads(r.content.decode('UTF-8'))["content"]

def update_transaction_cost(donationID, transactionCost, authToken=""):
    headers = {"Authorization": authToken, 'Content-Type': 'application/json'}
    payload = {
        "transactionCost": transactionCost
    }

    r = requests.put(
        f'http://localhost:3000/donations/transaction_cost/{donationID}',
        headers=headers,
        data=json.dumps(payload)
    )

    return json.loads(r.content.decode('UTF-8'))