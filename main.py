import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from pihmalawi_config import config
from datetime import datetime
from colorama import Fore, Style


def get_org_unit(facility: str) -> str:
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
    if facility.lower().startswith("tedzani"):
        return "sKN6JyTFe9M"


if __name__ == "__main__":
    print("\t\t\tNDP Reports Status")
    for each_endpoint in config["endpoints"]:
        report_config_df = pd.read_csv(each_endpoint["config_file"])
        report_df = pd.read_excel(each_endpoint["report_file"], header=0)
        report_df["orgUnit"] = report_df["FACILITY"].apply(get_org_unit)
        for index, row in report_df.iterrows():
            facility_report = {
                "dataSet": report_config_df["dataset"].iat[0],
                "completeDate": str(datetime.today().date()),
                "period": row["period"],
                "orgUnit": row["orgUnit"],
                "dataValues": [],
            }
            for idx, each_config in report_config_df.iterrows():
                column_name = each_config["excel_column"]
                has_category_combination = each_config["has_category_combination"]
                if has_category_combination == "yes":
                    category_option_combination = each_config[
                        "category_option_combination"
                    ]
                    column_name = column_name + "^" + category_option_combination

                if column_name in row:
                    data_element = {
                        "dataElement": each_config["dataset_element_id"],
                        "value": row[column_name],
                    }
                    if has_category_combination == "yes":
                        data_element["categoryOptionCombo"] = each_config[
                            "category_option_combination_id"
                        ]

                    facility_report["dataValues"].append(data_element)

            url = each_endpoint["base"] + report_config_df["resource"].iat[0]
            username = each_endpoint["username"]
            password = each_endpoint["password"]
            try:
                response = requests.post(
                    url,
                    json=facility_report,
                    auth=HTTPBasicAuth(username=username, password=password),
                )
                response.raise_for_status()
                print()
                if response.text.split(",")[1] != '"status":"SUCCESS"':
                    print(Fore.RED + "REPORT FOR ", row["FACILITY"], "NOT ADDED IN NDP")
                    print(
                        Fore.RED + "Please make sure all the columns for ",
                        row["FACILITY"],
                        "are filled with values",
                    )
                    print(Style.RESET_ALL)
                    # for debugging
                    # print(response.text)
                else:
                    print("Report for ", row["FACILITY"], "has been added in NDP")
            except Exception as e:
                print(e)
