import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import glob


# target file path
taget_file = "target_file.csv"

# log file path
log_file = "log_file.txt"


def __extract_csv(csv_file_path: str) -> pd.DataFrame:
    extracted_data = pd.read_csv(csv_file_path)
    return extracted_data


def __extract_json(json_file_path: str) -> pd.DataFrame:
    extracted_data = pd.read_json(json_file_path, lines=True)
    return extracted_data


def __extract_xml(xml_file_path: str) -> pd.DataFrame:
    extracted_data = pd.DataFrame(
        columns=["car_model", "year_of_manufacture" "price", "fuel"]
    )
    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    for row in root:
        car_model = row.find("car_model").text
        year_of_manufacture = row.find("year_of_manufacture").text
        price = float(row.find("price").text)
        fuel = row.find("fuel").text
        extracted_data = pd.concat(
            [
                extracted_data,
                pd.DataFrame(
                    [
                        {
                            "car_model": car_model,
                            "year_of_manufacture": year_of_manufacture,
                            "price": price,
                            "fuel": fuel,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
    return extracted_data


def extract() -> pd.DataFrame:

    all_extracted = pd.DataFrame(
        columns=["car_model", "year_of_manufacture" "price", "fuel"]
    )
    # extract all csv files:
    for csv_file in glob.glob("datasource/*.csv"):
        all_extracted = pd.concat(
            [all_extracted, __extract_csv(csv_file)], ignore_index=True
        )

    # extract all json files:
    for json_file in glob.glob("datasource/*.json"):
        all_extracted = pd.concat(
            [all_extracted, __extract_json(json_file)], ignore_index=True
        )

    # extract all xml files:
    for xml_file in glob.glob("datasource/*.xml"):
        all_extracted = pd.concat(
            [all_extracted, __extract_xml(xml_file)], ignore_index=True
        )

    return all_extracted


def transfrom(all_data_extracted):
    all_data_extracted["price"] = round(all_data_extracted["price"], 2)
    return all_data_extracted


def load(all_data_transformed: pd.DataFrame, target_file):
    all_data_transformed.to_csv(target_file)


def logging(message):
    datetime_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.strftime(datetime.now(), datetime_format)
    with open(log_file, "a") as file:
        file.write(now + " " + message + "\n")


# test ETL functions

# extract
all_data_extracted = extract()
logging("Data extracted successfully")

# transform
all_data_transformed = transfrom(all_data_extracted)
logging("Data transformed successfully")

# load
load(all_data_transformed, taget_file)
logging("Data loaded successfully")

logging("ETL process completed")
