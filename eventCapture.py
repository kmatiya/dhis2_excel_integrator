import pandas as pd
import requests
import json

from requests.auth import HTTPBasicAuth
from datetime import datetime


class EventManager:

    def __init__(self, url, report_config_df, username, password):
        self.__status_code = 0
        self.__url = url
        self.__report_config_df = report_config_df
        self.__username = username
        self.__password = password

    def get_attribute_option_combo(self, attributeOptionCombo):
        if attributeOptionCombo.lower() == "yes":
            return "hw3lundkEdl"
        if attributeOptionCombo.lower() == "no":
            return "cKkVX9bH3vB"

    def entered_event(self, event_date, org_unit):
        event_id = None
        try:
            params = {
                "program": self.__report_config_df["program"].iat[0],
                "orgUnit": org_unit
            }
            get_report = requests.get(self.__url, params=params,
                                      auth=HTTPBasicAuth(username=self.__username, password=self.__password))

            self.__status_code = get_report.status_code
            if self.__status_code == 200:
                report_json = json.loads(get_report.text)
                if len(report_json) > 0:
                    for event in report_json['events']:
                        if event['eventDate'] == event_date:
                            event_id = event["event"]
            else:
                print("Request was not successful to request data from NDP" + "for" + org_unit)
            return event_id
        except Exception as e:
            print(f"error in processing request:{e}")

    def upload_new_event(self, row):
        facility_report = {
            "program": self.__report_config_df["program"].iat[0],
            "attributeOptionCombo": row["attributeOptionCombo"],
            "completeDate": str(datetime.today().date()),
            "eventDate": row["Date"].strftime('%Y-%m-%dT%H:%M:%S'),
            "orgUnit": row["orgUnit"],
            "status": "COMPLETED",
            "dataValues": []
        }
        for idx, each_config in self.__report_config_df.iterrows():
            column_name = each_config["excel_column"]
            if column_name in row:
                data_element = {
                    "dataElement": each_config["program_element_id"],
                    "value": row[column_name]
                }
                facility_report["dataValues"].append(data_element)

        print(facility_report)
        post_result = requests.post(self.__url, json=facility_report,
                                    auth=HTTPBasicAuth(username=self.__username, password=self.__password))
        if post_result.status_code == 200:
            print("Report for " + row["FACILITY"] + " has been added in NDP")
        else:
            print("Error loading " + row["FACILITY"] + ". " + post_result.text)

    def update_event(self, event_id, row):
        try:
            url = self.__url+"/"+event_id
            report = requests.get(url, auth=HTTPBasicAuth(username=self.__username, password=self.__password))
            event = json.loads(report.text)
            facility_report = {
                "program": self.__report_config_df["program"].iat[0],
                "attributeOptionCombo": row["attributeOptionCombo"],
                "completeDate": str(datetime.today().date()),
                "eventDate": row["Date"].strftime('%Y-%m-%dT%H:%M:%S'),
                "orgUnit": row["orgUnit"],
                "status": "COMPLETED",
                "dataValues": []
            }
            for idx, each_config in self.__report_config_df.iterrows():
                column_name = each_config["excel_column"]
                if column_name in row:
                    data_element = {
                        "dataElement": each_config["program_element_id"],
                        "value": row[column_name]
                    }
                    facility_report["dataValues"].append(data_element)
            if facility_report["orgUnit"] == event["orgUnit"]:
                url = url+"/"+event_id
                update_report = requests.put(url, json=facility_report,
                                            auth=HTTPBasicAuth(username=self.__username, password=self.__password))
                self.__status_code = update_report.status_code
                if self.__status_code == 200:
                    print(f"Successfully updated Report for: {event['orgUnitName']} for date: {facility_report['eventDate']} ")
                else:
                    print(f"Error updating Report for: {facility_report['orgUnitName']} "
                          f"for date: {facility_report['eventDate']}" + update_report.text)

        except Exception as e:
            print(f"Error updating Report: {event_id} {e}")
