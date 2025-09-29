import csv
import json

with open("../docs/EIC_CODES.csv", "r") as file:
    reader = csv.reader(file)
    lines = [l for l in reader]
    file.close()

codes = {}

for l in lines[1:]:

    eic_code, eic_display_name, eic_long_name, countries, notes = l
    countries = countries.replace("\"", "").replace(",", "").split(" ")

    codes[eic_code] = {
        "eic_display_name": eic_display_name,
        "eic_long_name": eic_long_name,
        "countries": countries,
    }


filename = "../EIC-CODES.json"

with open(filename, "w") as json_file:
    json.dump(codes, json_file, indent=4)
