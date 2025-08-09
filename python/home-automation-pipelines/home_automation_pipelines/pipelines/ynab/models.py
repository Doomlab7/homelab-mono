from typing import Optional

from pydantic import BaseModel


class YNABRegister(BaseModel):
    Account: str
    # Flag: ignore
    # Check Number: ignore
    Date: str  # MM/DD/YY
    Memo: Optional[str] = ""
    Payee: str
    Category: str
    Outflow: float
    Inflow: float


class YNABRowWithExtras(BaseModel):
    Date: str  # MM/DD/YY
    Payee: Optional[str] = "UNKNOWN"
    Category: Optional[str] = None  # Master Category:Sub category
    Memo: Optional[str] = None
    Amount: float
    Other: str
    NumMatches: Optional[int] = 0


class ChaseCreditCardRow(BaseModel):
    TransactionDate: str  # MM/DD/YY
    # PostDate: ignore
    Description: str  #  map to payee using KNOWN_MAPPING
    Category: str  # Shopping, Food & Drink
    Type: str  # Sale, ...
    Amount: float  # will be negative for purchases
    # Memo: ignore
    #


class CEFCURow(BaseModel):
    # AccountNumber: str  # ignore
    PostDate: str  # MM/DD/YY
    # Check: str  # ignore
    Description: str  # map to payee using KNOWN_MAPPING
    Debit: float
    Credit: float
    # Status: str
    # Balance: float


class Capital1CreditCardRow(BaseModel):
    TransactionDate: str  # MM/DD/YY
    # PostDate: ignore
    # Cart No.: ignore
    Description: str  #  map to payee using KNOWN_MAPPING
    Category: str  # Shopping, Food & Drink
    Debit: float
    Credit: float


class FoxComRow(BaseModel):
    # AccountNumber: str  # ignore
    PostDate: str  # MM/DD/YY
    # Check: str  # ignore
    Description: str  # map to payee using KNOWN_MAPPING
    Debit: float
    Credit: float
    # Status: str
    # Balance: float
