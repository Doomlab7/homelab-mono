import re
from datetime import datetime
from typing import List

import pandas as pd
from tqdm import tqdm

from home_automation_pipelines.pipelines.ynab.cefcu.cefcu_known_payees import (
    KNOWN_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.models import CEFCURow
from home_automation_pipelines.pipelines.ynab.models import YNABRegister
from home_automation_pipelines.pipelines.ynab.models import YNABRowWithExtras


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
        return date_object.strftime("%m/%d/%Y")
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
    ynab_register_df = ynab_register_df[
        ["Account", "Date", "Payee", "Category", "Outflow", "Inflow", "Memo"]
    ]
    filtered_df = ynab_register_df[ynab_register_df["Account"] == account_filter]
    # Clean up Outflow and Inflow columns in ynab DataFrame
    filtered_df["Outflow"] = filtered_df["Outflow"].apply(clean_amount)
    filtered_df["Inflow"] = filtered_df["Inflow"].apply(clean_amount)
    # Normalize date columns in both DataFrames
    filtered_df["Date"] = filtered_df["Date"].apply(normalize_date)

    combined_df = merge_splits(filtered_df)
    return combined_df


def make_ynab_register_models(filtered_df: pd.DataFrame) -> List[YNABRegister]:
    ynab_register_data = []
    for _, row in filtered_df.iterrows():
        instance = YNABRegister(
            Account=row["Account"],
            Date=row["Date"],
            Payee=row["Payee"],
            Category=row["Category"],
            Outflow=row["Outflow"],
            Inflow=row["Inflow"],
        )
        ynab_register_data.append(instance)

    return ynab_register_data

    # Description	Debit	Credit	Status	Balance
    # WE ENERGIES 11/27/2023	185.14		Pending
    # ACH TRANSACTION PWP PAYPAL *SWA	226.02		Posted	16669
    # ACH TRANSACTION ALTRUIST FINANCI	100		Posted	16895.02


def read_cefcu_data(file_path: str) -> List[CEFCURow]:
    """Read cefcu data and create instances of CEFCURow."""
    cefcu_df = pd.read_csv(file_path)

    cefcu_df["Post Date"] = cefcu_df["Post Date"].apply(normalize_date)
    return cefcu_df[["Post Date", "Description", "Debit", "Credit"]]


def make_cefcu_models(cefcu_df: pd.DataFrame) -> List[CEFCURow]:
    cefcu_data = []
    for _, row in cefcu_df.iterrows():
        try:
            instance = CEFCURow(
                PostDate=row["Post Date"],
                Description=row["Description"],
                Debit=row["Debit"],
                Credit=row["Credit"],
            )
        except Exception:
            breakpoint()
        cefcu_data.append(instance)

    return cefcu_data


def join_and_format_data(
    ynab_register_data: List[YNABRegister], cefcu_data: List[CEFCURow], known_mapping
) -> List[YNABRowWithExtras]:
    """Join and format data according to YNABRow schema."""
    joined_data = []

    for cefcu_row in tqdm(cefcu_data):
        payee_mapping = known_mapping.get(cefcu_row.Description, "")

        potential_matches = []
        amount = cefcu_row.Credit if cefcu_row.Credit > 0 else -1 * cefcu_row.Debit
        relevant_ynab_register_data = [
            x
            for x in ynab_register_data
            if (abs((pd.to_datetime(cefcu_row.PostDate) - pd.to_datetime(x.Date)).days))
            <= 1
        ]
        for ynab_row in relevant_ynab_register_data:
            amount_difference = (
                abs(amount - ynab_row.Inflow)
                if amount >= 0
                else abs(ynab_row.Outflow + amount)
            )
            if amount_difference <= 1:
                # exact match already in
                if cefcu_row.Description == ynab_row.Memo:
                    break

                if cefcu_row.Description == "DIVIDEND CREDIT":
                    ynab_row.Category = "Income:Available this month"
                    ynab_row.Payee = "CEFCU"
                potential_matches.append(ynab_row)

        # check if there's an exact match in potential matches
        exact_matches = [
            ynab_row
            for ynab_row in potential_matches
            if abs(float(ynab_row.Outflow)) == abs(float(amount))
        ]
        if exact_matches:
            # no need to save this
            continue
            # exact_match: YNABRowWithExtras = exact_matches[0]
            # joined_row = YNABRowWithExtras(
            #     Date=cefcu_row.PostDate,
            #     Payee=payee_mapping,
            #     Category=exact_match.Category,
            #     Memo=cefcu_row.Description,
            #     Amount=amount,
            #     Other="Exact Match - YNAB Register",
            #     NumMatches=1,
            #
            # )
            #
            # joined_data.append(joined_row)
            # continue

        for ynab_row in potential_matches:
            joined_row = YNABRowWithExtras(
                Date=cefcu_row.PostDate,
                Payee=payee_mapping,
                Category=ynab_row.Category,
                Memo=cefcu_row.Description,
                Amount=amount,
                Other=f"Potential Match - YNAB Register: {ynab_row.Date}, Amount: {ynab_row.Inflow if amount >= 0 else ynab_row.Outflow}",
                NumMatches=len(potential_matches),
            )

            joined_data.append(joined_row)
        if not potential_matches:
            joined_row = YNABRowWithExtras(
                Date=cefcu_row.PostDate,
                Payee=payee_mapping,
                Category=None,
                Memo=cefcu_row.Description,
                Amount=amount,
                Other="No match",
            )
            joined_data.append(joined_row)

    return joined_data


def save_to_csv(data: List[YNABRowWithExtras], output_path: str):
    """Convert data to DataFrame and save to CSV."""
    data_df = pd.DataFrame([model.dict() for model in data])
    # filter data_df for where Other != Exact Match - YNAB Register unless I'm uploading to a fresh budget in ynab
    data_df = data_df[data_df["Other"] != "Exact Match - YNAB Register"]
    data_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/ynab_data/TheBudget as of 2023-12-28 1008 AM-Register.csv"
    # cefcu_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/cefcu/raw/AccountHistory_20231126.csv"
    cefcu_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/cefcu/raw/AccountHistory.csv"
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv_path = f"/home/nic/personal/home-automation-pipelines/data/ynab/outputs/raw-outputs/cefcu_joined_{now}.csv"

    # Define account filter
    account_filter = "CEFCU - Checking"

    # Read data
    ynab_register_df = read_ynab_register(ynab_register_file_path, account_filter)
    cefcu_df = read_cefcu_data(cefcu_file_path)

    # Merge DataFrames on 'date' and 'amount' columns
    # merged_df = pd.merge(cefcu_df, ynab_register_df, left_on=['Post Date', 'Credit'], right_on=["Date", "Inflow"], how='left', indicator=True)
    merged_df = pd.merge(
        cefcu_df,
        ynab_register_df,
        left_on=["Post Date", "Description"],
        right_on=["Date", "Memo"],
        how="left",
        indicator=True,
    )

    # Filter rows that are only in 'cefcu'
    filtered_cefcu = merged_df[merged_df["_merge"] == "left_only"].drop(
        "_merge", axis=1
    )

    # merged_df2 = pd.merge(filtered_cefcu, ynab_register_df, left_on=['Post Date', 'Debit'], right_on=["Date", "Outflow"], how='left', indicator=True)
    merged_df2 = pd.merge(
        filtered_cefcu,
        ynab_register_df,
        left_on=["Post Date", "Description"],
        right_on=["Date", "Memo"],
        how="left",
        indicator=True,
    )

    # Filter rows that are only in 'cefcu'
    filtered_cefcu2 = merged_df2[merged_df2["_merge"] == "left_only"].drop(
        "_merge", axis=1
    )
    cefcu_data = make_cefcu_models(filtered_cefcu2)
    ynab_register_data = make_ynab_register_models(ynab_register_df)

    # Join and format data
    joined_data = join_and_format_data(ynab_register_data, cefcu_data, KNOWN_MAPPING)

    # Save to CSV
    save_to_csv(joined_data, output_csv_path)
