import re
from datetime import datetime
from typing import List

import pandas as pd

from home_automation_pipelines.pipelines.ynab.chase.category_mapping import (
    CATEGORY_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.chase.chase_known_payees import (
    KNOWN_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.models import ChaseCreditCardRow
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


def read_chase_data(file_path: str) -> List[ChaseCreditCardRow]:
    """Read chase data and create instances of ChaseCreditCardRow."""
    chase_df = pd.read_csv(file_path)

    chase_df["Date"] = chase_df["Transaction Date"].apply(normalize_date)
    # outflows are purchases
    chase_df["Outflow"] = chase_df[chase_df["Amount"] < 0.0]["Amount"].fillna(0.0)
    chase_df["Outflow"] = abs(chase_df["Outflow"])
    # inflows are payments
    chase_df["Inflow"] = chase_df[chase_df["Amount"] > 0.0]["Amount"].fillna(0.0)
    chase_df["Inflow"] = abs(chase_df["Inflow"])
    chase_df["Memo"] = chase_df["Description"]
    return chase_df[["Date", "Memo", "Outflow", "Inflow"]]


if __name__ == "__main__":
    ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/ynab_data/TheBudget as of 2023-12-28 1008 AM-Register.csv"
    # chase_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/chase/raw/AccountHistory_20231126.csv"

    chase_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/chase/raw/Chase7288_Activity20220101_20231126_20231126.CSV"
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv_path = f"/home/nic/personal/home-automation-pipelines/data/ynab/outputs/raw-outputs/chase_joined_{now}.csv"

    # Define account filter
    account_filter = "Nic - Amazon Credit Card (7288)"

    # Read data
    ynab = read_ynab_register(ynab_register_file_path, account_filter)
    chase = read_chase_data(chase_file_path)

    ynab = ynab[ynab.Date > pd.to_datetime("2022-01-01")]
    chase = chase[chase.Date > pd.to_datetime("2022-01-01")]

    # Merge DataFrames on 'date' and 'amount' columns
    # merged_df = pd.merge(chase_df, ynab_register_df, left_on=['Transaction Date', 'Credit'], right_on=["Date", "Inflow"], how='left', indicator=True)
    merged_df = pd.merge(
        chase, ynab, on=["Date", "Outflow", "Inflow"], how="left", indicator=True
    )

    # Filter rows that are only in 'chase'
    filtered_chase = merged_df[merged_df["_merge"] == "left_only"].drop(
        "_merge", axis=1
    )
    filtered_chase["Memo"] = filtered_chase["Memo_x"]

    filtered_chase["Payee"] = filtered_chase["Memo"].map(KNOWN_MAPPING)

    filtered_chase["Category"] = filtered_chase["Memo"].map(CATEGORY_MAPPING)

    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("Amazon")), "Payee"
    ] = "Amazon"
    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("WALGREENS")), "Payee"
    ] = "Walgreens"
    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("UNITED      ")), "Payee"
    ] = "United"

    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("TST* HAPPY BELLIES BAKE S")), "Payee"
    ] = "Happy Bellies"

    filtered_chase.loc[(filtered_chase["Memo"].str.contains("AMZ")), "Payee"] = "Amazon"

    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("DOORDASH")), "Payee"
    ] = "Doordash"

    filtered_chase.loc[(filtered_chase["Memo"].str.contains("BP#")), "Payee"] = "BP"
    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("BP#")), "Category"
    ] = "Everday expenses:Fuel"
    filtered_chase.loc[
        (filtered_chase["Memo"] == "EL AZTECA"), "Category"
    ] = "Everyday Expenses:Out To Eat"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "Building For kids"), "Category"
    ] = "Everyday Expenses:General Kids Expenses"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "FLC*Olivet Bible Church"), "Payee"
    ] = "Olivet Bible Church"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "FLC*Olivet Bible Church"), "Category"
    ] = "Giving:Church"

    filtered_chase.loc[(filtered_chase["Memo"] == "EL AZTECA"), "Payee"] = "El Azteca"

    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("DOORDASH")), "Category"
    ] = "Everyday Expenses:Out To Eat"

    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("AIRBNB  ")), "Category"
    ] = "Large Expenses:General Savings (6 months = ~18k)"

    filtered_chase.loc[
        (filtered_chase["Memo"].str.contains("AIRBNB  ")), "Payee"
    ] = "Airbnb"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "FESTIVAL FOODS"), "Category"
    ] = "Everyday Expenses:Groceries"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "MYTHEDACARE"), "Category"
    ] = "Large Expenses:Medical"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "SQ *LAWLSS COFFEE APPLETO"), "Category"
    ] = "Everyday Expenses:Coffee Houses"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "SQ *LAWLSS COFFEE APPLETO"), "Category"
    ] = "Everyday Expenses:Coffee Houses"

    filtered_chase.loc[
        (filtered_chase["Memo"] == "SQ *COPPER ROCK COFFEE"), "Category"
    ] = "Everyday Expenses:Coffee Houses"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "Jimmy Johns"), "Category"
    ] = "Everyday Expenses:Out To Eat"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "Chick-Fil-A"), "Category"
    ] = "Everyday Expenses:Out To Eat"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "Frontiers"), "Category"
    ] = "Giving:Missions - Tim and Brenda"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "Flashback"), "Category"
    ] = "Monthly Bills:BJJ"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "Ace"), "Category"
    ] = "Large Expenses:House Maintenance"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "Spectrum"), "Category"
    ] = "Monthly Bills:Internet"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "Amazon"), "Category"
    ] = "Everyday Expenses:Home Supplies"

    filtered_chase.loc[
        (filtered_chase["Payee"] == "CVS"), "Category"
    ] = "Everyday Expenses:Medications"

    filtered_chase.loc[
        filtered_chase["Memo"] == "PWP*Privacy.com Business", "Payee"
    ] = "Privacy.com"

    filtered_chase.loc[
        filtered_chase["Memo"] == "AUTOMATIC PAYMENT - THANK", "Payee"
    ] = "Transfer:CEFCU - Checking"

    filtered_chase.loc[
        filtered_chase["Memo"] == "Payment Thank You - Web", "Payee"
    ] = "Transfer:CEFCU - Checking"

    filtered_chase.loc[filtered_chase["Memo"] == "TOMS CENTRAL", "Payee"] = "Tom's"

    # format Date to be MM/DD/YYYY
    filtered_chase["Date"] = filtered_chase["Date"].dt.strftime("%m/%d/%Y")

    filtered_chase[["Date", "Payee", "Category", "Memo", "Inflow", "Outflow"]].to_csv(
        output_csv_path, index=False
    )
