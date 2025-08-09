import re
from datetime import datetime
from typing import List

import pandas as pd

from home_automation_pipelines.pipelines.ynab.capital1.capital1_known_payees import (
    KNOWN_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.capital1.category_mapping import (
    CATEGORY_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.models import Capital1CreditCardRow
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
        try:
            # Convert the date string to datetime and then back to the desired format
            date_object = datetime.strptime(date_str, "%m/%d/%Y")
        except ValueError:
            date_object = datetime.strptime(date_str, "%Y-%m-%d")
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


def read_capital1_data(file_path: str) -> List[Capital1CreditCardRow]:
    """Read capital1 data and create instances of Capital1CreditCardRow."""
    capital1_df = pd.read_csv(file_path)

    capital1_df["Date"] = capital1_df["Transaction Date"].apply(normalize_date)
    # outflows are purcapital1s
    #
    capital1_df["Outflow"] = capital1_df["Debit"].fillna(0.0)
    capital1_df["Inflow"] = capital1_df["Credit"].fillna(0.0)
    capital1_df["Memo"] = capital1_df["Description"]
    return capital1_df[["Date", "Memo", "Outflow", "Inflow"]]


if __name__ == "__main__":
    ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/ynab_data/TheBudget as of 2023-12-28 1008 AM-Register.csv"

    capital1_file_path1 = "/home/nic/personal/home-automation-pipelines/data/ynab/capital1/raw/transactions_2022.csv"
    capital1_file_path2 = "/home/nic/personal/home-automation-pipelines/data/ynab/capital1/raw/transactions_2023_to_dec3.csv"

    # Define account filter
    account_filter = "Kassia - Capital One (0135)"

    # Define account filter
    account_filter = "Nic - Amazon Credit Card (7288)"

    for capital1_file_path in [capital1_file_path1, capital1_file_path2]:
        print(capital1_file_path)
        now = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

        output_csv_path = f"/home/nic/personal/home-automation-pipelines/data/ynab/outputs/raw-outputs/capital1_joined_{now}.csv"

        # Read data
        ynab = read_ynab_register(ynab_register_file_path, account_filter)
        capital1 = read_capital1_data(capital1_file_path)

        ynab = ynab[ynab.Date > pd.to_datetime("2022-01-01")]
        capital1 = capital1[capital1.Date > pd.to_datetime("2022-01-01")]

        # Merge DataFrames on 'date' and 'amount' columns
        # merged_df = pd.merge(capital1_df, ynab_register_df, left_on=['Transaction Date', 'Credit'], right_on=["Date", "Inflow"], how='left', indicator=True)
        merged_df = pd.merge(
            capital1, ynab, on=["Date", "Outflow", "Inflow"], how="left", indicator=True
        )

        # Filter rows that are only in 'capital1'
        filtered_capital1 = merged_df[merged_df["_merge"] == "left_only"].drop(
            "_merge", axis=1
        )
        filtered_capital1["Memo"] = filtered_capital1["Memo_x"]

        filtered_capital1["Payee"] = filtered_capital1["Memo"].map(KNOWN_MAPPING)

        filtered_capital1["Category"] = filtered_capital1["Memo"].map(CATEGORY_MAPPING)

        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("Amazon")), "Payee"
        ] = "Amazon"
        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("WALGREENS")), "Payee"
        ] = "Walgreens"
        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("TST* HAPPY BELLIES BAKE S")),
            "Payee",
        ] = "Happy Bellies"

        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("AMZ")), "Payee"
        ] = "Amazon"

        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("DOORDASH")), "Payee"
        ] = "Doordash"

        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("BP#")), "Payee"
        ] = "BP"
        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("BP#")), "Category"
        ] = "Everday expenses:Fuel"
        filtered_capital1.loc[
            (filtered_capital1["Memo"] == "EL AZTECA"), "Category"
        ] = "Everyday Expenses:Out To Eat"

        filtered_capital1.loc[
            (filtered_capital1["Memo"] == "EL AZTECA"), "Payee"
        ] = "El Azteca"

        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("DOORDASH")), "Category"
        ] = "Everyday Expenses:Out To Eat"

        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("AIRBNB  ")), "Category"
        ] = "Large Expenses:General Savings (6 months = ~18k)"

        filtered_capital1.loc[
            (filtered_capital1["Memo"].str.contains("AIRBNB  ")), "Payee"
        ] = "Airbnb"

        filtered_capital1.loc[
            (filtered_capital1["Memo"] == "FESTIVAL FOODS"), "Category"
        ] = "Everyday Expenses:Groceries"

        filtered_capital1.loc[
            (filtered_capital1["Memo"] == "SQ *COPPER ROCK COFFEE"), "Category"
        ] = "Everyday Expenses:Coffee Houses"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Jimmy Johns"), "Category"
        ] = "Everyday Expenses:Out To Eat"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Chick-Fil-A"), "Category"
        ] = "Everyday Expenses:Out To Eat"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Amazon"), "Category"
        ] = "Everyday Expenses:Home Supplies"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "CVS"), "Category"
        ] = "Everyday Expenses:Medications"

        filtered_capital1.loc[
            filtered_capital1["Memo"] == "AUTOMATIC PAYMENT - THANK", "Payee"
        ] = "Transfer:CEFCU - Checking"

        filtered_capital1.loc[
            filtered_capital1["Memo"] == "CAPITAL ONE AUTOPAY PYMT", "Payee"
        ] = "Transfer:CEFCU - Checking"
        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Aldi"), "Category"
        ] = "Everyday Expenses:Groceries"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Instacart"), "Category"
        ] = "Everyday Expenses:Groceries"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Kiwk Trip"), "Category"
        ] = "Everyday Expenses:Fuel"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Pet Supplies Plus"), "Category"
        ] = "Everyday Expenses:Cat Bills"
        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Petsmart"), "Category"
        ] = "Everyday Expenses:Cat Bills"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Scooter's Coffee"), "Category"
        ] = "Everyday Expenses:Coffee Houses"
        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "City of Appleton"), "Category"
        ] = "Everyday Expenses:Parking"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "TrueValue"), "Category"
        ] = "Everyday Expenses:Home Supplies"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Reveal"), "Category"
        ] = "Monthly Bills:Fitness"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Walgreens"), "Category"
        ] = "Everyday Expenses:General Kids Expenses"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Target"), "Category"
        ] = "Everyday Expenses:General Kids Expenses"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Dollar General"), "Category"
        ] = "Everyday Expenses:General Kids Expenses"
        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Goodwill"), "Category"
        ] = "Everyday Expenses:General Kids Expenses"

        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Michaels"), "Category"
        ] = "Everyday Expenses:General Kids Expenses"
        filtered_capital1.loc[
            (filtered_capital1["Payee"] == "Kiwk Trip"), "Category"
        ] = "Everyday Expenses:Fuel"

        # format Date to be MM/DD/YYYY
        filtered_capital1["Date"] = filtered_capital1["Date"].dt.strftime("%m/%d/%Y")

        print(output_csv_path)
        filtered_capital1[
            ["Date", "Payee", "Category", "Memo", "Inflow", "Outflow"]
        ].to_csv(output_csv_path, index=False)
