import json
import sys
from datetime import datetime

import pandas as pd

from database import client
from filestorage import fsc

db = client.get_database("busserebatetraces")
dwh = db.get_collection("data_warehouse")


def run_discrepancy_search(
    key, month, year, file_name=None, contracts=None, parts=None
):
    try:
        kwargs = json.loads(fsc.get_file("busserebatetracing", key))
    except Exception as e:
        print("key", key)
        raise e

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

    if file_name is not None:
        filter["__file__"] = {
            "$regex": r"^" + file_name,
            "$options": "i",
        }

    if contracts is None or len(contracts) == 0:
        contracts = None
    else:
        contracts = list(set(contracts.split(" ")))
        contracts = "^({})".format("|".join(contracts))
        filter[kwargs.get("contract")] = {
            "$regex": contracts,
            "$options": "i",
        }

    if parts is None or len(parts) == 0:
        part = None
    else:
        parts = list(set(parts.split(" ")))

        part = "^({})".format("|".join([str(x) for x in parts]))
        if key == "input/atlantic_med.json":
            part = "^({})".format("BUS".join([str(x) + "|" for x in parts]))

        filter["$expr"] = {
            "$regexMatch": {
                "input": {"$toString": "${}".format(kwargs.get("part"))},
                "regex": part,
            }
        }

    docs = list(
        dwh.find(filter, {"_id": 0}).sort(
            [(kwargs.get("contract"), 1), (kwargs.get("part"), 1)]
        )
    )

    if len(docs) == 0:
        sys.exit("No data found.")

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

    base_file_name = key.split("/")[1]
    file = f"discrepancies.{base_file_name}.{datetime.now():%Y%m%d%H%S}.xlsx"

    with pd.ExcelWriter(
        file,
        engine="xlsxwriter",
    ) as w:
        df.to_excel(w, index=False, sheet_name="Discrepancies")

    return df, file
