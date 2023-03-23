import json
import re
from datetime import datetime

import pandas as pd

from database import client
from filestorage import fsc

# add to end of spec file

# import shutil
# shutil.copyfile('config.yaml', '{0}/config.yaml'.format(DISTPATH))

db = client.get_database("busserebatetraces")
dwh = db.get_collection("data_warehouse")

ff = {
    "henryschein": "henryschein.json",
    "cardinal": "cardinal.json",
    "medline": "medline.json",
    "mgm": "mgm_weekly.json",
    "mckesson": "mgm_weekly.json",
    "concordance": "concordance.json",
    "mms": "concordance_mms.json",
    "dealmed": "dealmed.json",
    "tri_anim": "tri_anim.json",
    "om": "om.json",
    "owensminor": "om.json",
    "twinmed": "twin_med.json",
    "ndc": "ndc.json",
    "ndc_net": "ndc_net.json",
    "mohawk": "mohawk.json",
    "atl_med": "atlantic_med.json",
    "sentry": "sentry.json",
}


def fancy_print(values: list[str]) -> list[str]:
    output = []
    temp = ""

    for idx, v in enumerate(values):
        temp += " " * 12 + v

        if idx % 3 == 0 and idx > 0:
            output.append(temp)
            temp = ""

    if temp != "":
        output.append(temp)

    return output


month_regex = re.compile(r"^(0[1-9]|1[012])$")
year_regex = re.compile(r"^(19|20)\d\d$")


def cli():
    global ff

    print()
    month = input("Enter month (MM): (ctrl+c/z/d to quit) > ").strip()
    assert month_regex.match(month), "Invalid month"
    print()
    year = input("Enter year (YYYY): (ctrl+c/z/d to quit) > ").strip()
    assert year_regex.match(year), "Invalid year"

    print()
    print("Month", month)
    print("Year", year)
    print()

    print()
    print("Available distributor keys:")
    print()

    for f in fancy_print(list(ff.keys())):
        print(f)

    print()

    key = "input/"
    try:
        key += ff[input("Enter key: (ctrl+c/z/d to quit) > ").lower().strip()]
        base_file_name = key.split("/")[1]
    except KeyError:
        print("Invalid key")
        return

    try:
        kwargs = json.loads(fsc.get_file("busserebatetracing", key))
    except Exception:
        print("key", key)
        print("File does not exist")
        return

    filter = kwargs.get("filter")

    filter["__month__"] = month
    filter["__year__"] = year

    if key == "input/concordance.json":
        filter["__file__"] = {
            "$regex": r"^concordance(?!.*mms)",
            "$options": "i",
        }
    elif key == "input/concordance_mms.json":
        filter["__file__"] = {
            "$regex": r"^concordance.*mms",
            "$options": "i",
        }
    elif key == "input/mgm_weekly.json":
        filter["__file__"] = {
            "$regex": r"^(mgm|mckesson)",
            "$options": "i",
        }
    elif key == "input/ndc.json":
        filter["__file__"] = {
            "$regex": r"^ndc(?!.*_net)",
            "$options": "i",
        }
    elif key == "input/ndc_net.json":
        filter["__file__"] = {
            "$regex": r"^(ndc_net)",
            "$options": "i",
        }
    elif key == "input/om.json":
        filter["__file__"] = {
            "$regex": r"^(om)",
            "$options": "i",
        }
    else:
        filter["__file__"] = {
            "$regex": r"^" + key.split("/")[1].split(".")[0][:3],
            "$options": "i",
        }

    print()
    print("File", key)
    print()

    print("Separate multiple with empty spaces")
    contracts = list(set(input("Enter contracts: (ctrl+c/z/d to quit) > ").split(" ")))
    if contracts == "":
        contracts = None
    else:
        contracts = "^({})".format("|".join(contracts))
        filter[kwargs.get("contract")] = {
            "$regex": contracts,
            "$options": "i",
        }

    print()
    print("Contract(s)", contracts)
    print()

    print("Separate multiple with empty spaces")
    parts = list(set(input("Enter part: (ctrl+c/z/d to quit) > ").split(" ")))
    if parts == "":
        part = None
    else:
        part = "^({})".format("|".join([str(x) for x in parts]))
        if key == "input/atlantic_med.json":
            part = "^({})".format("BUS".join([str(x) + "|" for x in parts]))

        filter["$expr"] = {
            "$regexMatch": {
                "input": {"$toString": "${}".format(kwargs.get("part"))},
                "regex": part,
            }
        }

    print()
    print("Part(s)", part)
    print()

    print()
    print("Filter:")
    for key, value in filter.items():
        print("\t{", key, "=>", value, "}")
    print()

    continue_or_break = input("Continue? (y/n): (ctrl+c/z/d to quit) > ").lower() in [
        "y",
        "yes",
    ]
    if not continue_or_break:
        print("Exiting...")
        return

    docs = list(
        dwh.find(filter, {"_id": 0}).sort(
            [(kwargs.get("contract"), 1), (kwargs.get("part"), 1)]
        )
    )

    print()
    print("Found", len(docs), "rows")
    print()

    if len(docs) == 0:
        print("Exiting...")

    df = pd.DataFrame(docs)

    if kwargs.get("discrepancies_cols", None) is not None:
        col_map = kwargs.get("discrepancies_cols")
        c = list(col_map.values())
        columns = []
        for x in c:
            if isinstance(x, str):
                columns.append(x)
            else:
                columns.append("79")

        df = df[columns]
        df = df.fillna("")
        df.columns = list(col_map.keys())
        df["Cardinal Invoice Date"] = pd.to_datetime(
            df["Cardinal Invoice Date"], format="%Y%m%d"
        )
        df["Cardinal Invoice Date"] = df["Cardinal Invoice Date"].dt.strftime(
            "%m/%d/%Y"
        )
        df["Error Code"] = "CC"

    preview = input("Preview? (y/n): (ctrl+c/z/d to quit) > ").lower() in ["y", "yes"]
    if preview:
        print()
        print("Sample")
        print(df.head())
        print()

    save_or_break = input("Save? (y/n): (ctrl+c/z/d to quit) > ").lower() in [
        "y",
        "yes",
    ]
    if not save_or_break:
        print("Exiting...")
        return

    file = f"discrepancies.{base_file_name}.{datetime.now():%Y%m%d%H%S}.xlsx"

    print()
    print("Saving to", file)
    print()

    with pd.ExcelWriter(
        file,
        engine="xlsxwriter",
    ) as w:
        df.to_excel(w, index=False, sheet_name="Discrepancies")


if __name__ == "__main__":
    # cli()
    from gui import *
