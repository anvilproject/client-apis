"""
Splits .ndjson files into individual .json files
"""

import os
import json

from dotenv import load_dotenv

load_dotenv()

# env constants
OUTPUT_DIR = os.getenv("OUTPUT_PATH", "./data")

FHIR_RESOURCE_TYPES = [
    "DocumentReference",
    "Organization",
    "Patient",
    "Practitioner",
    "ResearchStudy",
    "ResearchSubject",
    "Specimen",
    "Task",
]

for res_type in FHIR_RESOURCE_TYPES:
    # create directory for each res type
    path = os.path.join(os.getcwd(), OUTPUT_DIR, res_type)
    if not os.path.exists(path):
        os.mkdir(path)

    # open the res type file
    fhir_file = open(f"{OUTPUT_DIR}/{res_type}.ndjson", "r")
    fhir_resources = fhir_file.readlines()
    count = 0
    total = len(fhir_resources)
    for res in fhir_resources:
        json_dict = json.loads(res)
        res_id = json_dict["id"]

        count += 1
        print(f"({count}/{total}) {res_id}")

        with open(f"{OUTPUT_DIR}/{res_type}/{res_id}.json", "w+") as outfile:
            outfile.write(res)
            outfile.close()
