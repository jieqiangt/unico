import pandas as pd
import numpy as np
import math
import requests

BASE_URL = "https://www.onemap.gov.sg/api"

def get_token(user, pw):

    endpoint = "auth/post/getToken"
    payload = {
        "email": user,
        "password": pw,
    }

    response = requests.request("POST", f'{BASE_URL}/{endpoint}', json=payload)

    if response.ok:
        return response.json()

    print(response.text)


def search_postal(postal_code):

    endpoint = "common/elastic/search"
    headers = {'cache-control': 'no-cache, max-age=0',
               'content-type': 'application/json'}
    params = {"searchVal": str(postal_code), "returnGeom": "Y",
              "getAddrDetails": "Y", "pageNum": 1}

    response = requests.get(url=f"{BASE_URL}/{endpoint}",
                            params=params, headers=headers)

    return response

def format_postal_search_result(result, bp_code, postal_code):

    result_list = result.json()["results"]
    returned_address = pd.DataFrame.from_dict(result_list[0], orient="index").T
    returned_address.drop(columns=['X','Y'],inplace=True)
    returned_address['bp_code'] = bp_code
    returned_address['INPUT_POSTAL'] = postal_code
    returned_address['QUERY_STATUS'] = "SUCCESS"
    
    new_col_names = []
    for col in returned_address.columns:
        new_col_names.append(col.lower())
        
    returned_address.columns = new_col_names

    return returned_address

def return_postal_search_result(bp_code, postal_code):

    print(f'{bp_code} : {postal_code}')
    try:
        float(postal_code)
    except ValueError:
        return pd.DataFrame.from_dict({"bp_code": bp_code, "BLK_NO": "", "ROAD_NAME": "", "BUILDING": "", "ADDRESS": "",
                                       "POSTAL": "", "INPUT_POSTAL": postal_code, "LATITUDE": np.nan, "LONGITUDE": np.nan, "QUERY_STATUS": "INPUT POSTAL HAS LETTERS"}, orient="index").T

    if math.isnan(float(postal_code)):
        return pd.DataFrame.from_dict({"bp_code": bp_code, "BLK_NO": "", "ROAD_NAME": "", "BUILDING": "", "ADDRESS": "",
                                       "POSTAL": "", "INPUT_POSTAL": postal_code, "LATITUDE": np.nan, "LONGITUDE": np.nan, "QUERY_STATUS": "INPUT POSTAL IS NULL"}, orient="index").T

    api_result = search_postal(postal_code)

    if api_result.ok:
        result_dict = api_result.json()

    if len(result_dict["results"]) == 0:
        return pd.DataFrame.from_dict({"bp_code": bp_code, "BLK_NO": "", "ROAD_NAME": "", "BUILDING": "", "ADDRESS": "",
                                       "POSTAL": "", "INPUT_POSTAL": postal_code, "LATITUDE": np.nan, "LONGITUDE": np.nan, "QUERY_STATUS": "NO_RESULTS"}, orient="index").T
        
    else:
        return format_postal_search_result(api_result, bp_code, postal_code)