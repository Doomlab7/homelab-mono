import re
from datetime import datetime
from typing import List

import pandas as pd

from home_automation_pipelines.pipelines.ynab.cefcu.category_mapping import (
    CATEGORY_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.cefcu.cefcu_known_payees import (
    KNOWN_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.models import CEFCURow
from home_automation_pipelines.pipelines.ynab.models import YNABRegister


def merge_splits(df):
    df = df.fillna("")
    # filter for rows with "split" in the memo columns
    split_df = df[df["Memo"].str.contains("split")]
    # group by date and sum the outflow and inflow
    grouped_df = (
        split_df.groupby("Date").agg({"Outflow": "sum", "Inflow": "sum"}).reset_index()
    )
    # replace all the rows in df that were sliced out with grouped_df
    df.loc[split_df.index, "Outflow"] = grouped_df["Outflow"]
    df.loc[split_df.index, "Inflow"] = grouped_df["Inflow"]
    # dedup on date, payee, outflow, inflow
    df = df.drop_duplicates(subset=["Date", "Payee", "Outflow", "Inflow"], keep="first")

    return df


#
# Function to normalize date format
def normalize_date(date_str):
    if pd.notna(date_str):
        # Convert the date string to datetime and then back to the desired format
        date_object = datetime.strptime(date_str, "%m/%d/%Y")
        return pd.to_datetime(date_object.strftime("%m/%d/%Y"))
    else:
        return None


# Function to clean up Outflow and Inflow columns
def clean_amount(amount):
    if pd.notna(amount):
        return float(re.sub(r"[$,]", "", amount))
    else:
        return 0.0


def read_ynab_register(file_path: str, account_filter: str) -> List[YNABRegister]:
    """Read YNAB register file, filter based on account, and create instances of YNABRegister."""
    ynab_register_df = pd.read_csv(file_path)
    filtered_df = ynab_register_df[ynab_register_df["Account"] == account_filter]
    filtered_df = filtered_df[
        ["Date", "Payee", "Category", "Outflow", "Inflow", "Memo"]
    ]
    # Clean up Outflow and Inflow columns in ynab DataFrame
    filtered_df["Outflow"] = filtered_df["Outflow"].apply(clean_amount).fillna(0.0)
    filtered_df["Inflow"] = filtered_df["Inflow"].apply(clean_amount).fillna(0.0)
    # Normalize date columns in both DataFrames
    filtered_df["Date"] = filtered_df["Date"].apply(normalize_date)

    combined_df = merge_splits(filtered_df)
    return combined_df


def read_cefcu_data(file_path: str) -> List[CEFCURow]:
    """Read cefcu data and create instances of CEFCURow."""
    cefcu_df = pd.read_csv(file_path)

    cefcu_df["Date"] = cefcu_df["Post Date"].apply(normalize_date)
    cefcu_df["Outflow"] = cefcu_df["Debit"].fillna(0.0)
    cefcu_df["Inflow"] = cefcu_df["Credit"].fillna(0.0)
    cefcu_df["Memo"] = cefcu_df["Description"]
    return cefcu_df[["Date", "Memo", "Outflow", "Inflow"]]


if __name__ == "__main__":
    ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/ynab_data/TheBudget as of 2024-05-27 153 PM-Register.csv"
    # cefcu_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/cefcu/raw/AccountHistory_20231126.csv"
    cefcu_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/cefcu/raw/AccountHistory.csv"
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv_path = f"/home/nic/personal/home-automation-pipelines/data/ynab/outputs/raw-outputs/cefcu_joined_{now}.csv"

    # Define account filter
    account_filter = "CEFCU - Checking"

    # Read data
    ynab = read_ynab_register(ynab_register_file_path, account_filter)
    cefcu = read_cefcu_data(cefcu_file_path)

    ynab = ynab[ynab.Date > pd.to_datetime("2023-01-01")]
    cefcu = cefcu[cefcu.Date > pd.to_datetime("2023-01-01")]

    # Merge DataFrames on 'date' and 'amount' columns
    # merged_df = pd.merge(cefcu_df, ynab_register_df, left_on=['Post Date', 'Credit'], right_on=["Date", "Inflow"], how='left', indicator=True)
    merged_df = pd.merge(
        cefcu, ynab, on=["Date", "Outflow", "Inflow"], how="left", indicator=True
    )
    print(merged_df.head())

    # Filter rows that are only in 'cefcu'
    filtered_cefcu = merged_df[merged_df["_merge"] == "left_only"].drop(
        "_merge", axis=1
    )
    filtered_cefcu["Memo"] = filtered_cefcu["Memo_x"]

    filtered_cefcu["Payee"] = filtered_cefcu["Memo"].map(KNOWN_MAPPING)

    filtered_cefcu["Category"] = filtered_cefcu["Memo"].map(CATEGORY_MAPPING)

    filtered_cefcu.loc[
        (filtered_cefcu["Outflow"] == 700)
        & (filtered_cefcu["Memo"] == "ACH TRANSACTION VENMO"),
        "Category",
    ] = "Giving:Misc (501 (c)(3))"

    filtered_cefcu.loc[
        (filtered_cefcu["Outflow"] == 375)
        & (filtered_cefcu["Memo"] == "ACH TRANSACTION VENMO"),
        "Category",
    ] = "Debt:Parent PLUS"

    filtered_cefcu.loc[
        (filtered_cefcu["Outflow"] == 375)
        & (filtered_cefcu["Memo"] == "ACH TRANSACTION VENMO"),
        "Payee",
    ] = "Transfer:Parent PLUS"

    # format Date to be MM/DD/YYYY
    filtered_cefcu["Date"] = filtered_cefcu["Date"].dt.strftime("%m/%d/%Y")

    filtered_cefcu[["Date", "Payee", "Category", "Memo", "Inflow", "Outflow"]].to_csv(
        output_csv_path, index=False
    )
