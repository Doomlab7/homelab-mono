"""

"""

import pandas as pd
from fuzzywuzzy import process
from rich import print_json

from home_automation_pipelines.pipelines.ynab.cefcu.cefcu_known_payees import (
    KNOWN_MAPPING,
)

data_path = "/home/nic/personal/home-automation-pipelines/data/ynab/cefcu/raw/AccountHistory_20231126.csv"
register_path = (
    "/mnt/nas/dump/YNAB testing/TheBudget as of 2023-11-24 238 PM-Register.csv"
)

known_businesses = list(set(pd.read_csv(register_path)["Payee"].tolist()))
cefcu_payees = list(set(pd.read_csv(data_path)["Description"].tolist()))

for k, v in KNOWN_MAPPING.items():
    try:
        cefcu_payees.pop(k)
    except:
        pass
    try:
        known_businesses.pop(v)
    except:
        pass


# Function to find the best match for each known business in the credit card payees
def generate_mapping(known, credit_card):
    mapping = KNOWN_MAPPING.copy()
    for credit_card_payee in credit_card:
        if str(credit_card_payee) == "nan":
            continue
        if "AMZN Mk" in credit_card_payee:
            mapping[credit_card_payee] = "Amazon"
            continue
        elif credit_card_payee in KNOWN_MAPPING.keys():
            continue

        try:
            # Use process.extractOne to get the best match and its similarity score
            best_match, score = process.extractOne(credit_card_payee, known)
        except Exception as e:
            print(f"Error: {e}\n{credit_card_payee=}")
            continue

        # You can set a threshold for the similarity score to consider a match
        if score >= 80:
            mapping[credit_card_payee] = best_match
        else:
            print(f"NOT GOOD ENOUGH: {best_match=} for {credit_card_payee}, {score=}")
            mapping[credit_card_payee] = None

    return mapping


if __name__ == "__main__":
    # Generate the mapping
    business_mapping = generate_mapping(known_businesses, cefcu_payees)
    print_json(
        data={
            k: v for k, v in business_mapping.items() if k not in KNOWN_MAPPING.keys()
        }
    )
    # Path("/home/nic/personal/home-automation-pipelines/home_automation_pipelines/pipelines/ynab/cefcu/cefcu_known_payees.py").write_text(json.dumps(business_mapping))
# # Print the mapping
#     for credit_card_payee, known in business_mapping.items():
# print(f"{credit_card_payee=} -> {known=}")
