import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

clerks_df = pd.read_excel('./IC3-Clerks.xlsx', header=0)
ndpClerkUrl = "https://neno.pih-emr.org/dhis/api/dataValueSets"
username = "pih_dhis2_user"
pwd = "pih_dhis_password"

dataSetDictionary = {
    "Number of DBS samples rejected": "NX9ITIOkqCw",
    "Number of exposed infants registered": "poyokFLTPMo",
    "Number of HIV exposed infants born": "WYUB8EK3P02",
    "Number of lab results received and entered in the viral load register": "nyj0DMQn5kk",
    "Number of mastercards requested": "d0SdXKBG0Rv",
    "Number of patients entered in the TRACE update forms": "GZlIeseXO8P",
    "Number of patients that had visited after their appointment on a non IC3 clinic day": "wO1eI3ZhtiF",
    "Number of viral load plasma samples rejected": "gVP7uekbIS2",
    "Number of viral load requests filled": "OyGjf17Iht6",
    "Number of exposed infants received": "EAwRze4dQ58"
}


def get_org_unit(facility):
    facility = str(facility)
    if facility.lower().startswith("chifunga"):
        return "pciHYsH4glX"
    if facility.lower().startswith("lisungwi"):
        return "jBJ1nrUXKIu"
    if facility.lower().startswith("matope"):
        return "GjNQ12Y2l0F"
    if facility.lower().startswith("midzemba"):
        return "zq5yo5iRvsL"
    if facility.lower().startswith("nkula"):
        return "cfzBcWqPOoy"
    if facility.lower().startswith("zalewa"):
        return "NW5K84KJ4xp"
    if facility.lower().startswith("dambe"):
        return "OhKdUBApLZa"
    if facility.lower().startswith("ligowe"):
        return "gA0WGnhCnYt"
    if facility.lower().startswith("luwani"):
        return "y3FF95NnZzl"
    if facility.lower().startswith("magaleta"):
        return "NFqFeBSH2Re"
    if facility.lower().startswith("matandani"):
        return "JKAFWLrwdji"
    if facility.lower().startswith("neno district"):
        return "Rmh4wKR794k"
    if facility.lower().startswith("neno parish"):
        return "I4Vox6oteWl"
    if facility.lower().startswith("nsambe"):
        return "HxziIaDjatq"


if __name__ == '__main__':
    print(clerks_df.head())
    clerks_df["orgUnit"] = clerks_df["Facility"].apply(get_org_unit)
    for index, row in clerks_df.iterrows():
        facility_report = {
            "dataSet": "XttTY8YbYKB",
            "completeDate": "2021-02-16",
            "period": row["period"],
            "orgUnit": row["orgUnit"],
            "dataValues": [
                {
                    "dataElement": dataSetDictionary["Number of viral load requests filled"],
                    "value": row["# of Total VL request filled"]
                },
                {
                    "dataElement": dataSetDictionary[
                        "Number of lab results received and entered in the viral load register"],
                    "value": row["# of Lab results received and entered in the VL register"]
                },
                {
                    "dataElement": dataSetDictionary["Number of patients entered in the TRACE update forms"],
                    "value": row["# of Patients entered in TRACE update form"]
                },
                {
                    "dataElement": dataSetDictionary[
                        "Number of patients that had visited after their appointment on a non IC3 clinic day"],
                    "value": row["# of Patients that had visited after their appointment on a non-IC3 day"]
                },
                {
                    "dataElement": dataSetDictionary["Number of mastercards requested"],
                    "value": row["# of master cards requested"]
                },
                {
                    "dataElement": dataSetDictionary["Number of HIV exposed infants born"],
                    "value": row["# of HIV exposed infants born (Maternity register)"]
                },
                {
                    "dataElement": dataSetDictionary["Number of exposed infants registered"],
                    "value": row["# of exposed registered this month (EID Register)"]
                },
                {
                    "dataElement": dataSetDictionary["Number of DBS samples rejected"],
                    "value": row["# of DBS sample rejected"]
                },
                {
                    "dataElement": dataSetDictionary["Number of viral load plasma samples rejected"],
                    "value": row["# of VL Plasma sample rejected"]
                },
                {
                    "dataElement": dataSetDictionary["Number of exposed infants received"],
                    "value": row["# of  Exposed infants received"]
                }
            ]
        }
        post_result = requests.post(ndpClerkUrl, json=facility_report,
                                    auth=HTTPBasicAuth(username=username, password=pwd))
        if post_result.status_code == 200:
            print(post_result.text)
            print("Report for " + row["Facility"] + " has been added in NDP")
        else:
            print("Error loading " + row["Facility"] + ". " + post_result.text)
