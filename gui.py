from datetime import datetime

import dearpygui.dearpygui as dpg

from discrepancies import run_discrepancy_search

now = datetime.now()
current_year = now.year
last_year = current_year - 1

field_files_dict = {
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

month = ""
year = ""
key = "input/"
file_name = ""
contracts = ""
items = ""
header = []
sample = []
file = ""


def save_callback():
    global month, year, key, file_name, contracts, items, sample, header, file

    if file_name == "":
        file_name = None

    df, file = run_discrepancy_search(key, month, year, file_name, contracts, items)

    dpg.set_value(output_file_name, file)

    for idx, row in enumerate(df.head().to_dict("records")):
        if idx == 0:
            header = list(row.keys())
        sample.append(list(row.values()))

    with dpg.window(label="Sample Output", pos=[400, 0], width=1200, height=400):
        with dpg.table(header_row=False):
            for i in range(len(header)):
                dpg.add_table_column()

            for i in range(len(sample)):
                with dpg.table_row():
                    for j in range(len(sample[i])):
                        dpg.add_text(sample[i][j])


def set_month(sender, data):
    global month
    month = data


def set_year(sender, data):
    global year
    year = data


def set_key(sender, data):
    global key
    key = "input/" + field_files_dict.get(data, "")


def set_custom_file(sender, data):
    global file_name
    file_name = data


def set_contracts(sender, data):
    global contracts
    contracts = data


def set_items(sender, data):
    global items
    items = data


dpg.create_context()
dpg.create_viewport(width=1600, height=400, title="Discrepancy Search")

with dpg.window(
    label="Discrepancies",
    pos=[0, 0],
    width=400,
    height=400,
):
    dpg.add_combo(
        label="* Month (MM)",
        items=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
        callback=set_month,
    )
    dpg.add_combo(
        label="* Year (YYYY)",
        items=[str(last_year), str(current_year)],
        callback=set_year,
    )
    dpg.add_combo(
        label="* Distributor", items=list(field_files_dict.keys()), callback=set_key
    )

    dpg.add_text("Optional: Use a Custom File Name")
    dpg.add_input_text(label="File Name", callback=set_custom_file)

    dpg.add_text("Enter Contracts and Items to Search")
    dpg.add_text("Leave blank to search all contracts and items")
    dpg.add_text("Separate Multiple with Spaces ' '")
    dpg.add_text("Eg.) R13135 R21020 or 279 648 649")
    dpg.add_input_text(label="Contract(s)", callback=set_contracts)
    dpg.add_input_text(label="Item(s)", callback=set_items)

    dpg.add_text("Click Save to run the discrepancy search")
    dpg.add_button(label="Save", callback=save_callback)

    output_file_name = dpg.add_text(file)


dpg.setup_dearpygui()
dpg.show_viewport()
dpg.start_dearpygui()
