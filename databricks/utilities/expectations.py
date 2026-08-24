CUSTOMER_EXPECTATIONS = {
    "valid_customer_id_format":
        "customer_id RLIKE '^CUST[0-9]{6}$'",

    "valid_full_name":
        "full_name is not NULL AND TRIM(full_name) !=''",

    "emirates_id_present":
        "emirates_id is not NULL AND TRIM(emirates_id) !=''",

    "valid_emirates_id_format":
        r"emirates_id RLIKE '^[0-9]{3}-[0-9]{4}-[0-9]{7}-[0-9]{1}$'",

    "valid_nationality":
        "nationality is not NULL AND TRIM(nationality) !=''",

    "dob_not_in_future":
        "date_of_birth <= current_date()",

    "account_opened_date_not_in_future":
        "account_opened_date <= current_date()",

    "account_opened_after_dob":
        "account_opened_date >= date_of_birth",

    "valid_risk_tier":
        "risk_tier IN('LOW', 'MEDIUM', 'HIGH')",

    "valid_email_format":
        r"email is NULL OR email RLIKE '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]+$'",

    "valid_phone_format":
        r"phone is NULL OR phone RLIKE '^[+]971-[0-9]{2}-[0-9]{3}-[0-9]{4}$'",

    "valid_adf_run_id":
        "adf_run_id IS NOT NULL AND TRIM(adf_run_id) != ''",

    "valid_ingested_at":
        "_ingested_at IS NOT NULL",

    "valid_source_file":
        "source_file is not NULL AND TRIM(source_file) != ''"

}

ACCOUNT_EXPECTATIONS = {
    "valid_account_id_format":
        "account_id RLIKE '^ACC[0-9]{7}$'",

    "valid_customer_id_format":
        "customer_id RLIKE '^CUST[0-9]{6}$'",

    "valid_account_type":
        "account_type IN ('SALARY', 'SAVINGS', 'CREDIT_CARD', 'CURRENT')",

    "valid_account_status":
        "account_status IN ('ACTIVE', 'DORMANT', 'CLOSED')",

    "valid_account_currency":
        "account_currency IN ('AED', 'EUR', 'USD')",

    "valid_opened_required":
        "opened_date IS NOT NULL",

    "valid_opened_date":
        "opened_date <= current_date()",

    "valid_adf_run_id":
        "adf_run_id IS NOT NULL AND TRIM(adf_run_id) != ''",

    "valid_ingested_at":
        "_ingested_at IS NOT NULL",

    "valid_source_file":
        "source_file is not NULL AND TRIM(source_file) != ''"

}

TRANSACTION_EXPECTATIONS = {
    "valid_transaction_id":
        "transaction_id is not NULL AND transaction_id RLIKE '^TXN([0-9]{14})$'",

    "valid_card_id":
        "card_id is not NULL AND card_id RLIKE '^CARD[0-9]{6}$'",

    "valid_customer_id":
        "customer_id is not NULL AND customer_id RLIKE '^CUST[0-9]{6}$'",

    "valid_account_id":
        "account_id is not NULL AND account_id RLIKE '^ACC[0-9]{7}$'",

    "valid_transaction_amount":
        "transaction_amount is not NULL AND transaction_amount > 0",

    "valid_transaction_timestamp":
        "transaction_timestamp is not NULL AND transaction_timestamp <= current_timestamp()",

    "valid_account_currency":
        "account_currency is not NULL AND account_currency IN ('AED', 'EUR', 'USD')",
        
    "valid_city":
        "city is NULL OR city RLIKE '^[\\p{L}]+([ .''-][\\p{L}]+)*$'",

    "valid_transaction_status":
        "transaction_status IN ('APPROVED', 'DECLINED')",

    "valid_adf_run_id":
        "adf_run_id is not NULL AND TRIM(adf_run_id) != ''",

    "valid_ingested_at":
        "_ingested_at IS NOT NULL",

    "valid_mcc_code":
        "mcc_code is NULL OR mcc_code BETWEEN 1000 AND 9999",

    "valid_merchant":
        "merchant IS NULL OR TRIM(merchant) != ''",

    "valid_auth_code":
        "transaction_status != 'APPROVED' OR (auth_code is not NULL AND auth_code RLIKE '^AUTH[0-9]{6}$')",

    "valid_source_file":
        "source_file is not NULL AND TRIM(source_file) != ''"
}

SWIPES_CARD_EXPECTATIONS ={
    "valid_card_id":
        "card_id is not NULL AND card_id RLIKE '^CARD[0-9]{6}$'",

    "valid_customer_id":
        "customer_id is not NULL AND customer_id RLIKE '^CUST[0-9]{6}$'",

    "valid_account_id":
        "account_id is not NULL AND account_id RLIKE '^ACC[0-9]{7}$'",

    "valid_swipe_id":
        "swipe_id is not NULL AND swipe_id RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'",

    "valid_card_swipe_amount":
        "card_swipe_amount is not NULL AND card_swipe_amount > 0",

    "valid_swipe_timestamp":
        "swipe_timestamp is not NULL AND swipe_timestamp <= current_timestamp()",

    "valid_country":
        "country IS NOT NULL AND TRIM(country) != ''",

    "valid_city":
        r"city is NULL OR city RLIKE '^[\p{L}]+([ .''-][\p{L}]+)*$'",

    "valid_swipe_card_currency":
        "swipe_card_currency is not NULL AND swipe_card_currency IN ('AED', 'EUR', 'USD')",

    "valid_mcc_code":
        "mcc_code is NULL OR mcc_code BETWEEN 1000 AND 9999",

    "valid_merchant":
        "merchant IS NULL OR TRIM(merchant) != ''",

    "valid_source_file":
        "source_file is not NULL AND TRIM(source_file) != ''",

    "valid_adf_run_id":
        "adf_run_id is not NULL AND TRIM(adf_run_id) != ''",

    "valid_ingested_at":
        "_ingested_at IS NOT NULL"
}

SANCTIONS_LIST_EXPECTATIONS = {
    "valid_entity_type":
        "entity_type is not NULL AND entity_type IN ('INDIVIDUAL', 'ORGANIZATION')",

    "valid_list_source":
        "list_source is not NULL AND list_source IN ('EU Sanctions (mock)', 'UN Consolidated List','OFAC SDN (mock)')",

    "valid_name":
        "name is not NULL AND TRIM(name) != ''",

    "valid_entry_id":
        "entry_id IS NOT NULL AND entry_id RLIKE '^SANC[0-9]{4,}$'",

    "valid_source_file":
        "source_file is not NULL AND TRIM(source_file) != ''",

    "valid_adf_run_id":
        "adf_run_id is not NULL AND TRIM(adf_run_id) != ''",

    "valid_ingested_at":
        "_ingested_at IS NOT NULL"
}