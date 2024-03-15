import pandas as pd
from dataCapture import dataCapture
from pihmalawi_config import config
from eventCapture import EventManager


def get_report_type():
    for each_endpoint in config["endpoints"]:
        report_config_df = pd.read_csv(each_endpoint["config_file"])
        if report_config_df["resource"][0] == "/dataValueSets":
            dataCapture()
        if report_config_df["resource"][0] == "/events":
            print("starting Event manager")
            event_manager = EventManager(config)
            print("Checking for already entered reports with the same date")
            event_ids = event_manager.entered_events()
            if len(event_ids) != 0:
                print("There are Reports for the dates provided: Preparing to update existing reports")
                event_manager.update_event()
            else:
                print("Uploading Reports to NDP")
                event_manager.upload_new_event()


if __name__ == '__main__':
    get_report_type()