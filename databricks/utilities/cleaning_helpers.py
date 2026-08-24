from pyspark.sql import Column
from pyspark.sql.functions import (
    initcap,
    trim,
    regexp_replace,
    lower,
    upper,
    when,
    length,
    coalesce,
    to_date,
)

# clean customer full name
def clean_name(col_name: Column | str) -> Column:
    only_alpha = regexp_replace(col_name, "[^a-zA-Z\\s]", "")
    return trim(initcap(only_alpha))

# clean and upper text
def clean_text(col_name: Column | str) -> Column:
    return upper(trim(col_name))

#clean customer_id
def clean_id(col_name: Column | str) -> Column:
    allowed_letters = regexp_replace(col_name, "[^a-zA-Z0-9]","")
    return upper(trim(allowed_letters))

#formatted customer id
def clean_formatted_customer_id(col_name: Column | str) -> Column:
    cleaned_id = clean_id(col_name)
    formatted_id = regexp_replace(
        cleaned_id,
        r"^CUST([0-9]{6})$",
        r"CUST$1"
    )
    return formatted_id
#formatted account id
def clean_formatted_account_id(col_name: Column | str) -> Column:
    cleaned_id = clean_id(col_name)
    formatted_id = regexp_replace(
        cleaned_id,
        r"^ACC([0-9]{7})$",
        r"ACC$1"
    )
    return formatted_id
# formatted transaction id
def clean_formatted_transaction_id(col_name: Column | str) -> Column:
    cleaned_id = clean_id(col_name)
    formatted_id = regexp_replace(
        cleaned_id,
        r"^TXN([0-9]{14})$",
        r"TXN$1"
    )
    return formatted_id

# formatted card id
def clean_formatted_card_id(col_name: Column | str) -> Column:
    cleaned_id = clean_id(col_name)
    formatted_id = regexp_replace(
        cleaned_id,
        r"^CARD([0-9]{6})$",
        r"CARD$1"
    )
    return formatted_id

# Clean formatted customer emirates id (eid)
def clean_emirates_id(col_name: Column | str) -> Column:
    digit_only = regexp_replace(trim(col_name), "[^0-9]","")
    formatted_eid = regexp_replace(
        digit_only,
        r"^([0-9]{3})([0-9]{4})([0-9]{7})([0-9]{1})$",
        r"$1-$2-$3-$4"
    )
    return formatted_eid

# clean email
def clean_email(col_name: Column | str) -> Column:
    clean_email = regexp_replace(lower(trim(col_name)), r"[^a-zA-Z0-9_@.%+-]", "")
    return clean_email

# clean and formatted phone
def clean_phone(col_name: Column | str) -> Column:
    digit_only = regexp_replace(trim(col_name), "[^0-9]","")

    init_digit = when(digit_only.startswith("0"), regexp_replace(digit_only, "^0","971")).otherwise(digit_only)

    formatted_phone = regexp_replace(
        init_digit,
        "^([0-9]{3})([0-9]{2})([0-9]{3})([0-9]{4})$",
        "+$1-$2-$3-$4"
    )
    return formatted_phone

# clean nationality
def clean_nationality(col_name: Column | str) -> Column:
    cleaned = trim(col_name)
    return when(length(cleaned) <=3, upper(cleaned)).otherwise(initcap(lower(cleaned)))


"""
    Cleans dirty string date column and converts it into standard PySpark DateType (YYYY-MM-DD).
    Handles multiple date patterns: yyyy-MM-dd, dd/MM/yyyy, MM-dd-yyyy, yyyy/MM/dd
    """
def parse_mixed_date(col_name: Column | str) -> Column:
    cleaned = trim(col_name)
    return coalesce(
        to_date(cleaned, "yyyy-MM-dd"),   # e.g., 1991-01-17
        to_date(cleaned, "dd/MM/yyyy"),   # e.g., 17/01/1991
        to_date(cleaned, "MM-dd-yyyy"),   # e.g., 01-17-1991
        to_date(cleaned, "yyyy/MM/dd"),   # e.g., 1991/01/17
        to_date(cleaned)
    )

# clean curreny
def clean_account_currency(col_name: Column | str) -> Column:
    allowed_letter = regexp_replace(col_name, "[^a-zA-Z]","")
    return upper(trim(allowed_letter))

#clean account type
def clean_account_type(col_name: Column | str)-> Column:
    cleaned_type = regexp_replace(col_name, "[^a-zA-Z_]","")
    return upper(trim(cleaned_type))

#clean account status
def clean_account_status(col_name: Column | str)-> Column:
    cleaned_status = regexp_replace(col_name, "[^a-zA-Z]","")
    return upper(trim(cleaned_status))