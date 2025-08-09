"Use YNAB register to get list of payees and categories"
import json
from pathlib import Path

import pandas as pd

register_path = (
    "/mnt/nas/dump/YNAB testing/TheBudget as of 2023-11-23 705 AM-Register.csv"
)
data = Path(register_path)
data = pd.read_csv(register_path)

payees = data["Payee"].unique()
categories = data["Category"].unique()

payees_list = Path(
    "/home/nic/personal/home-automation-pipelines/home_automation_pipelines/pipelines/ynab/payees.json"
)
categories_list = Path(
    "/home/nic/personal/home-automation-pipelines/home_automation_pipelines/pipelines/ynab/categories.json"
)

payees_list.write_text(json.dumps(payees.tolist()))
categories_list.write_text(json.dumps(categories.tolist()))
