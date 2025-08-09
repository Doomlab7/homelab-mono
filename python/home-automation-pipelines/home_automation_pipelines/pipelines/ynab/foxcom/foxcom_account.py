import re
from datetime import datetime
from typing import List

import pandas as pd
from tqdm import tqdm

from home_automation_pipelines.pipelines.ynab.foxcom.foxcom_known_payees import (
    KNOWN_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.models import FoxComRow
from home_automation_pipelines.pipelines.ynab.models import YNABRegister
from home_automation_pipelines.pipelines.ynab.models import YNABRowWithExtras


def read_ynab_register(file_path: str, account_filter: str) -> List[YNABRegister]:
    """Read YNAB register file, filter based on account, and create instances of YNABRegister."""
    ynab_register_df = pd.read_csv(file_path)
    filtered_df = ynab_register_df[ynab_register_df["Account"] == account_filter]

    ynab_register_data = []
    for _, row in filtered_df.iterrows():
        # Remove "$" and convert to float for Outflow and Inflow
        outflow = (
            float(re.sub(r"[$,]", "", row["Outflow"]))
            if pd.notna(row["Outflow"])
            else 0.0
        )
        inflow = (
            float(re.sub(r"[$,]", "", row["Inflow"]))
            if pd.notna(row["Inflow"])
            else 0.0
        )

        instance = YNABRegister(
            Account=row["Account"],
            Date=row["Date"],
            Payee=row["Payee"],
            Category=row["Category"],
            Outflow=outflow,
            Inflow=inflow,
        )
        ynab_register_data.append(instance)

    return ynab_register_data


# TODO: refactor this to handle foxcom data now instead of credit card data
# Description	Debit	Credit	Status	Balance
# WE ENERGIES 11/27/2023	185.14		Pending
# ACH TRANSACTION PWP PAYPAL *SWA	226.02		Posted	16669
# ACH TRANSACTION ALTRUIST FINANCI	100		Posted	16895.02
def read_foxcom_data(file_path: str) -> List[FoxComRow]:
    """Read foxcom data and create instances of FoxComRow."""
    foxcom_df = pd.read_csv(file_path)
    foxcom_data = []
    for _, row in foxcom_df.iterrows():
        try:
            instance = FoxComRow(
                PostDate=row["Post Date"],
                Description=row["Description"],
                Debit=row["Debit"],
                Credit=row["Credit"],
            )
        except Exception:
            breakpoint()
        foxcom_data.append(instance)

    return foxcom_data


def join_and_format_data(
    ynab_register_data: List[YNABRegister], foxcom_data: List[FoxComRow], known_mapping
) -> List[YNABRowWithExtras]:
    """Join and format data according to YNABRow schema."""
    joined_data = []

    for foxcom_row in tqdm(foxcom_data):
        payee_mapping = known_mapping.get(foxcom_row.Description, "")

        potential_matches = []
        amount = foxcom_row.Credit if foxcom_row.Credit > 0 else -1 * foxcom_row.Debit
        for ynab_row in ynab_register_data:
            amount_difference = (
                abs(amount - ynab_row.Inflow)
                if amount >= 0
                else abs(ynab_row.Outflow + amount)
            )
            date_difference = abs(
                (
                    pd.to_datetime(foxcom_row.PostDate) - pd.to_datetime(ynab_row.Date)
                ).days
            )

            if amount_difference <= 1 and date_difference <= 3:
                potential_matches.append(ynab_row)

        # check if there's an exact match in potential matches
        exact_matches = [
            ynab_row
            for ynab_row in potential_matches
            if abs(ynab_row.Outflow) == abs(amount)
        ]
        if exact_matches:
            exact_match: YNABRowWithExtras = exact_matches[0]
            joined_row = YNABRowWithExtras(
                Date=foxcom_row.PostDate,
                Payee=payee_mapping,
                Category=exact_match.Category,
                Memo=foxcom_row.Description,
                Amount=amount,
                Other="Exact Match - YNAB Register",
                NumMatches=1,
            )

            joined_data.append(joined_row)
            continue

        for ynab_row in potential_matches:
            joined_row = YNABRowWithExtras(
                Date=foxcom_row.PostDate,
                Payee=payee_mapping,
                Category=ynab_row.Category,
                Memo=foxcom_row.Description,
                Amount=amount,
                Other=f"Potential Match - YNAB Register: {ynab_row.Date}, Amount: {ynab_row.Inflow if amount >= 0 else ynab_row.Outflow}",
                NumMatches=len(potential_matches),
            )

            joined_data.append(joined_row)
        if not potential_matches:
            joined_row = YNABRowWithExtras(
                Date=foxcom_row.PostDate,
                Payee=payee_mapping,
                Category=None,
                Memo=foxcom_row.Description,
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
    ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/ynab_data/TheBudget as of 2023-11-24 238 PM-Register.csv"
    foxcom_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/foxcom/raw/2023_to_dec_6.csv"
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv_path = f"/home/nic/personal/home-automation-pipelines/data/ynab/outputs/raw-outputs/foxcom_joined_{now}.csv"

    # Define account filter
    account_filter = "Fox Communities Credit Union (Checking)"

    # Read data
    ynab_register_data = read_ynab_register(ynab_register_file_path, account_filter)
    credit_card_data = read_foxcom_data(foxcom_file_path)

    # Join and format data
    joined_data = join_and_format_data(
        ynab_register_data, credit_card_data, KNOWN_MAPPING
    )

    # Save to CSV
    save_to_csv(joined_data, output_csv_path)
