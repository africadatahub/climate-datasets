from dotenv import load_dotenv
import requests
import json
import pandas as pd
import numpy as np
import argparse
import os
from functools import partial

load_dotenv()

# Define CKAN API endpoint and your API key]
fields = []
csv_path =os.getenv('disaster_filename')
csv_delimiter = ","
ckan_api_url = os.getenv('ckan_url')
api_key =  os.getenv('api_keys')
resource_id = os.getenv('disaster_resource_id')

def datastore_delete(ckan_api_url, resource_id, api_key):
    print('Deleting datastore for ' + resource_id)

    u = ckan_api_url + "/api/3/action/datastore_delete"
    r = requests.post(u, headers={
      "X-CKAN-API-Key": api_key,
      "Accept": "application/json",
      'Content-Type': 'application/json',
    }, data=json.dumps({
        "resource_id": resource_id,
        "force": "true"
    }))

    return json.loads(r.text)

def datastore_create(records, fields, ckan_api_url, resource_id, api_key):
    print('Creating new datastore for ' + resource_id)

    u = ckan_api_url + "/api/3/action/datastore_create"
    r = requests.post(u, headers={
      "X-CKAN-API-Key": api_key,
      "Accept": "application/json",
      'Content-Type': 'application/json',
    }, data=json.dumps({
        "resource_id": resource_id,
        "records": records,
        "fields": fields,
        "force": "true"
    }))
    return json.loads(r.text)

def update_resource(df, csv_path, ckan_api_url, resource_id, api_key):
    
    df.to_csv(csv_path, index=False)
    u = ckan_api_url + "/api/3/action/resource_patch"
    r = requests.post(u, headers={
      "X-CKAN-API-Key": api_key
    }, data={
        "id": resource_id
    }, files=[('upload', open('./' + csv_path, 'r'))])
    data_output = json.loads(r.content)
    print(data_output)

print('Fetching Data')

climatology_df = pd.read_csv('emdat_africa_1992-2023_natural.csv')
#print(climatology_df)

df = climatology_df

def preparing_climate_data():

    print('Preparing data...')
    climate_df = pd.DataFrame()
    records = []
    fields = [
            { 
                "id": "Dis No",
                "type": "text"
            },
            { 
                "id": "Year",
                "type": "numeric"
            },
            { 
                "id": "Seq",
                "type": "numeric"
            },
            { 
                "id": "Glide",
                "type": "text"
            },
            { 
                "id": "Disaster Group",
                "type": "text"
            },
            { 
                "id": "Disaster Subgroup",
                "type": "text"
            },
            { 
                "id": "Disaster Type",
                "type": "text"
            },
            { 
                "id": "Disaster Subtype",
                "type": "text"
            }
            ,
            { 
                "id": "Event Name",
                "type": "text"
            },
            { 
                "id": "Country",
                "type": "text"
            },
            { 
                "id": "ISO",
                "type": "text"
            },
            { 
                "id": "Region",
                "type": "text"
            },
            { 
                "id": "Continent",
                "type": "text"
            },
            { 
                "id": "Location",
                "type": "text"
            },
            { 
                "id": "Origin",
                "type": "text"
            },
            { 
                "id": "Associated Dis",
                "type": "text"
            },
            { 
                "id": "Associated Dis2",
                "type": "text"
            },
            { 
                "id": "OFDA Response",
                "type": "text"
            },
            { 
                "id": "Appeal",
                "type": "text"
            },
            { 
                "id": "Declaration",
                "type": "text"
            },
            { 
                "id": "AID Contribution ('000 US$)",
                "type": "numeric"
            },
            { 
                "id": "Dis Mag Value",
                "type": "numeric"
            },
            { 
                "id": "Dis Mag Scale",
                "type": "text"
            },
            { 
                "id": "Latitude",
                "type": "text"
            },
            { 
                "id": "Longitude",
                "type": "text"
            },
            { 
                "id": "Local Time",
                "type": "text"
            },
            { 
                "id": "River Basin",
                "type": "text"
            },
            { 
                "id": "Start Year",
                "type": "numeric"
            },
            { 
                "id": "Start Month",
                "type": "numeric"
            },
            { 
                "id": "Start Day",
                "type": "numeric"
            },
            { 
                "id": "End Year",
                "type": "numeric"
            },
            { 
                "id": "End Month",
                "type": "numeric"
            },
            { 
                "id": "End Day",
                "type": "numeric"
            },
            { 
                "id": "Total Deaths",
                "type": "numeric"
            },
            { 
                "id": "No Injured",
                "type": "numeric"
            },
            { 
                "id": "No Affected",
                "type": "numeric"
            },
            { 
                "id": "No Homeless",
                "type": "numeric"
            },
            { 
                "id": "Total Affected",
                "type": "numeric"
            },
            { 
                "id": "Reconstruction Costs ('000 US$)",
                "type": "numeric"
            },
            { 
                "id": "Reconstruction Costs, Adjusted ('000 US$)",
                "type": "numeric"
            },
            { 
                "id": "Insured Damages ('000 US$)",
                "type": "numeric"
            },
            { 
                "id": "Insured Damages, Adjusted ('000 US$)",
                "type": "numeric"
            },
            { 
                "id": "Total Damages ('000 US$)",
                "type": "numeric"
            },
            { 
                "id": "Total Damages, Adjusted ('000 US$)",
                "type": "numeric"
            },
            { 
                "id": "CPI",
                "type": "text"
            },
            { 
                "id": "Adm Level",
                "type": "text"
            },
            { 
                "id": "Admin1 Code",
                "type": "text"
            },
            { 
                "id": "Admin2 Code",
                "type": "text"
            },
            { 
                "id": "Geo Location",
                "type": "text"
            }
            
            
        ]

    update_resource(df, 'TAVG-climatology.csv', ckan_api_url,resource_id, api_key)

    for row in climate_df:

        records.append({
            "Dis No": row[0],
            "Year": row[1],
            "Seq": row[2],
            "Glide": row[3],
            "Disaster Group": row[4],
            "Disaster Subgroup": row[5],
            "Disaster Type": row[6],
            "Disaster Subtype": row[7],
            "Event Name": row[9],
            "Country": row[10],
            "ISO": row[11],
            "Region": row[12],
            "Continent": row[13],
            "Location": row[14],
            "Origin": row[15],
            "Associated Dis": row[16],
            "Associated Dis2": row[17],
            "OFDA Response": row[18],
            "Appeal": row[19],
            "Declaration": row[20],
            "AID Contribution ('000 US$)": row[21],
            "Dis Mag Value": row[22],
            "Dis Mag Scale": row[23],
            "Latitude": row[24],
            "Longitude": row[25],
            "Local Time": row[26],
            "River Basin": row[27],
            "Start Year": row[28],
            "Start Month": row[29],
            "Start Day": row[30],
            "End Year": row[31],
            "End Month": row[32],
            "End Day": row[33],
            "Total Deaths": row[34],
            "No Injured": row[35],
            "No Affected": row[36],
            "No Homeless": row[37],
            "Total Affected": row[38],
            "Reconstruction Costs ('000 US$)": row[39],
            "Reconstruction Costs, Adjusted ('000 US$)": row[40],
            "Insured Damages ('000 US$)": row[41],
            "Insured Damages, Adjusted ('000 US$)": row[42],
            "Total Damages ('000 US$)": row[43],
            "Total Damages, Adjusted ('000 US$)": row[44],
            "CPI": row[45],
            "Adm Level": row[46],
            "Admin1 Code": row[47],
            "Admin2 Code": row[48],
            "Geo Location": row[49],
        })

        
    response = datastore_create(records, fields, ckan_api_url, resource_id, api_key)
    print(response) 

datastore_delete(ckan_api_url, resource_id, api_key)
preparing_climate_data()
