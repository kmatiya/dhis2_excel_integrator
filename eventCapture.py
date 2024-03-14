import pandas as pd
import requests
import json

from requests.auth import HTTPBasicAuth
from pihmalawi_config import config
from datetime import datetime


class EventManager:

    def __init__(self, config):
        self.__status_code = 0
        self.__config = config
        self.__event_ids = []
        self.__reports = []

    def get_org_unit(self,facility):
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
        if facility.lower().startswith("neno dh"):
            return "Rmh4wKR794k"
        if facility.lower().startswith("neno parish"):
            return "I4Vox6oteWl"
        if facility.lower().startswith("nsambe"):
            return "HxziIaDjatq"

    def get_attribute_option_combo(self, attributeOptionCombo):
        attributeOptionCombo = str(attributeOptionCombo)
        if attributeOptionCombo.lower() == "yes":
            return "hw3lundkEdl"
        if attributeOptionCombo.lower() == "no":
            return "cKkVX9bH3vB"

    def entered_events(self):
        try:
            for each_endpoint in self.__config["endpoints"]:
                report_config_df = pd.read_csv(each_endpoint["config_file"])
                report_df = pd.read_excel(each_endpoint["report_file"], header=0)
                report_df["orgUnit"] = report_df["Facility"].apply(self.get_org_unit)
                url = each_endpoint["base"] + report_config_df['resource'].iat[0]
                username = each_endpoint["username"]
                password = each_endpoint["password"]

                for index, row in report_df.iterrows():
                    event_date = row["Date"].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                    params = {
                        "program": report_config_df["program"].iat[0],
                        "orgUnit": row["orgUnit"]
                    }
                    get_report = requests.get(url, params=params,
                                              auth=HTTPBasicAuth(username=username, password=password))

                    self.__status_code = get_report.status_code
                    if self.__status_code == 200:
                        report_json = json.loads(get_report.text)
                        if len(report_json) > 0:
                            for event in report_json['events']:
                                if event['eventDate'] == event_date:
                                    self.__event_ids.append(event["event"])
                    else:
                        print("Request was not successful to request data from NDP" + "for" + row["orgUnit"])
        except Exception as e:
            print(f"error in processing request:{e}")
        return self.__event_ids

    def update_event(self):
        event_id = 0
        try:
            for report_id in self.__event_ids:
                event_id = report_id
                for each_endpoint in self.__config["endpoints"]:
                    username = each_endpoint["username"]
                    password = each_endpoint["password"]
                    url = each_endpoint["base"]+"/events/"+event_id
                    report = requests.get(url, auth=HTTPBasicAuth(username=username,password=password))
                    event = json.loads(report.text)
                    report_file_df = pd.read_excel(each_endpoint["report_file"])
                    report_config_df = pd.read_csv(each_endpoint["config_file"])
                    report_file_df["orgUnit"] = report_file_df["Facility"].apply(self.get_org_unit)
                    report_file_df["attributeOptionCombo"] = report_file_df["Event Supported by GAC"].apply(
                        self.get_attribute_option_combo)
                    for index, row in report_file_df.iterrows():
                        facility_report = {
                            "program": report_config_df["program"].iat[0],
                            "attributeOptionCombo": row["attributeOptionCombo"],
                            "completeDate": str(datetime.today().date()),
                            "eventDate": row["Date"].strftime('%Y-%m-%dT%H:%M:%S'),
                            "orgUnit": row["orgUnit"],
                            "status": "COMPLETED",
                            "dataValues": []
                        }
                        for idx, each_config in report_config_df.iterrows():
                            column_name = each_config["excel_column"]
                            if column_name in row:
                                data_element = {
                                    "dataElement": each_config["program_element_id"],
                                    "value": row[column_name]
                                }
                                facility_report["dataValues"].append(data_element)
                        if facility_report["orgUnit"] == event["orgUnit"]:
                            url = each_endpoint["base"] + "/events/"+event_id
                            update_report = requests.put(url, json=facility_report,
                                                        auth=HTTPBasicAuth(username=username, password=password))
                            self.__status_code = update_report.status_code
                            if self.__status_code == 200:
                                print(f"Successfully updated Report for: {event['orgUnitName']} for date: {facility_report['eventDate']} ")
                            else:
                                print(f"Error updating Report for: {facility_report['orgUnitName']} "
                                      f"for date: {facility_report['eventDate']}" + update_report.text)

        except Exception as e:
            print(f"Error updating Report: {event_id} {e}")

    def upload_new_event(self):
        for each_endpoint in config["endpoints"]:
            report_config_df = pd.read_csv(each_endpoint["config_file"])
            report_df = pd.read_excel(each_endpoint["report_file"], header=0)
            report_df["orgUnit"] = report_df["Facility"].apply(self.get_org_unit)
            report_df["attributeOptionCombo"] = report_df["Event Supported by GAC"].apply(
                self.get_attribute_option_combo)
            print(report_df.head())

            for index, row in report_df.iterrows():
                facility_report = {
                    "program": report_config_df["program"].iat[0],
                    "attributeOptionCombo": row["attributeOptionCombo"],
                    "completeDate": str(datetime.today().date()),
                    "eventDate": row["Date"].strftime('%Y-%m-%dT%H:%M:%S'),
                    "orgUnit": row["orgUnit"],
                    "status": "COMPLETED",
                    "dataValues": []
                }
                for idx, each_config in report_config_df.iterrows():
                    column_name = each_config["excel_column"]
                    if column_name in row:
                        data_element = {
                            "dataElement": each_config["program_element_id"],
                            "value": row[column_name]
                        }
                        facility_report["dataValues"].append(data_element)

                print(facility_report)
                url = each_endpoint["base"] + report_config_df["resource"].iat[0]
                username = each_endpoint["username"]
                password = each_endpoint["password"]
                post_result = requests.post(url, json=facility_report,
                                            auth=HTTPBasicAuth(username=username, password=password))
                if post_result.status_code == 200:
                    print("Report for " + row["Facility"] + " has been added in NDP")
                else:
                    print("Error loading " + row["Facility"] + ". " + post_result.text)


