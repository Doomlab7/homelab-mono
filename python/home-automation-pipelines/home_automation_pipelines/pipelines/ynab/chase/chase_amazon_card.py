import re
from datetime import datetime
from typing import List

import pandas as pd
from tqdm import tqdm

from home_automation_pipelines.pipelines.ynab.chase.chase_known_payees import (
    KNOWN_MAPPING,
)
from home_automation_pipelines.pipelines.ynab.models import ChaseCreditCardRow
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


def read_credit_card_data(file_path: str) -> List[ChaseCreditCardRow]:
    """Read credit card data and create instances of ChaseCreditCardRow."""
    credit_card_df = pd.read_csv(file_path)
    credit_card_data = []
    for _, row in credit_card_df.iterrows():
        try:
            instance = ChaseCreditCardRow(
                TransactionDate=row["Transaction Date"],
                Description=row["Description"],
                Category=row["Category"],
                Type=row["Type"],
                Amount=row["Amount"],
            )
        except Exception:
            breakpoint()
        credit_card_data.append(instance)

    return credit_card_data


def join_and_format_data(
    ynab_register_data: List[YNABRegister],
    credit_card_data: List[ChaseCreditCardRow],
    known_mapping,
) -> List[YNABRowWithExtras]:
    """Join and format data according to YNABRow schema."""
    joined_data = []

    for credit_card_row in tqdm(credit_card_data):
        payee_mapping = known_mapping.get(credit_card_row.Description, "")

        potential_matches = []
        for ynab_row in ynab_register_data:
            amount_difference = (
                abs(credit_card_row.Amount - ynab_row.Inflow)
                if credit_card_row.Amount >= 0
                else abs(ynab_row.Outflow + credit_card_row.Amount)
            )
            date_difference = abs(
                (
                    pd.to_datetime(credit_card_row.TransactionDate)
                    - pd.to_datetime(ynab_row.Date)
                ).days
            )

            if amount_difference <= 2 and date_difference <= 3:
                if credit_card_row.Description == "AUTOMATIC PAYMENT - THANK":
                    # ynab_row.Category = "Transfer:CEFCU - Checking"
                    ynab_row.Category = "Transfer:CEFCU - Checking"
                elif credit_card_row.Description == "TOMS DRIVE IN E WISCO":
                    ynab_row.Category = "Everyday Expenses:Out To Eat"
                elif credit_card_row.Description == "CHEFOS PANCAKE HOUSE":
                    ynab_row.Category = "Giving:Misc (non deductible)"
                elif credit_card_row.Description == "AKAME SUSHI":
                    ynab_row.Category = "Everyday Expenses:Out To Eat"
                elif credit_card_row.Description == "WISCONSIN SWIM ACADEMY LL":
                    ynab_row.Category = "Everyday Expenses:General Kids Expenses"
                potential_matches.append(ynab_row)

        # check if there's an exact match in potential matches
        exact_matches = [
            ynab_row
            for ynab_row in potential_matches
            if abs(ynab_row.Outflow) == abs(credit_card_row.Amount)
        ]
        if exact_matches:
            exact_match: YNABRowWithExtras = exact_matches[0]
            joined_row = YNABRowWithExtras(
                Date=credit_card_row.TransactionDate,
                Payee=payee_mapping,
                Category=exact_match.Category,
                Memo=credit_card_row.Description,
                Amount=credit_card_row.Amount,
                Other="Exact Match - YNAB Register",
                NumMatches=1,
            )

            joined_data.append(joined_row)
            continue

        for ynab_row in potential_matches:
            joined_row = YNABRowWithExtras(
                Date=credit_card_row.TransactionDate,
                Payee=payee_mapping,
                Category=ynab_row.Category,
                Memo=credit_card_row.Description,
                Amount=credit_card_row.Amount,
                Other=f"Potential Match - YNAB Register: {ynab_row.Date}, Amount: {ynab_row.Inflow if credit_card_row.Amount >= 0 else ynab_row.Outflow}",
                NumMatches=len(potential_matches),
            )

            joined_data.append(joined_row)
        if not potential_matches:
            joined_row = YNABRowWithExtras(
                Date=credit_card_row.TransactionDate,
                Payee=payee_mapping,
                Category=None,
                Memo=credit_card_row.Description,
                Amount=credit_card_row.Amount,
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
    # Replace these paths with your actual file paths
    # ynab_register_file_path = "/mnt/nas/dump/YNAB testing/TheBudget as of 2023-11-24 238 PM-Register.csv"
    # ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab_register_chase_example.csv"
    # ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab_register_chase_example_toms.csv"
    ynab_register_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/ynab_data/TheBudget as of 2023-11-24 238 PM-Register.csv"
    # credit_card_file_path = "/mnt/nas/dump/YNAB testing/Chase7288_Activity20230101_20231124_20231124.CSV"
    # credit_card_file_path = "/home/nic/personal/home-automation-pipelines/data/chase_example.csv"
    # credit_card_file_path = "/home/nic/personal/home-automation-pipelines/data/chase_example_toms.csv"
    credit_card_file_path = "/home/nic/personal/home-automation-pipelines/data/ynab/chase/raw/Chase7288_Activity20220101_20231126_20231126.CSV"
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv_path = f"/home/nic/personal/home-automation-pipelines/data/ynab/outputs/raw-outputs/chase_joined_{now}.csv"

    # Define account filter
    account_filter = "Nic - Amazon Credit Card (7288)"

    # Read data
    ynab_register_data = read_ynab_register(ynab_register_file_path, account_filter)
    credit_card_data = read_credit_card_data(credit_card_file_path)

    # Join and format data
    joined_data = join_and_format_data(
        ynab_register_data, credit_card_data, KNOWN_MAPPING
    )

    # Save to CSV
    save_to_csv(joined_data, output_csv_path)
