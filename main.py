import pandas as pd
from dataCapture import dataCapture
from eventCapture import eventCapture
from pihmalawi_config import config

def get_report_type():
    for each_endpoint in config["endpoints"]:
        report_config_df = pd.read_csv(each_endpoint["config_file"])
        if report_config_df["resource"][0] == "/dataValueSets":
            dataCapture()
        if report_config_df["resource"][0] == "/events":
            eventCapture()

if __name__ == '__main__':
    get_report_type()