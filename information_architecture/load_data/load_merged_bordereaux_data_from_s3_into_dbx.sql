
copy into  claims_workspace.bronze.bordereaux_data 
from 
    (
        SELECT 
        policy_number,
        CAST(months_insured as BIGINT) as months_insured,
        has_claims,
        CAST(items_insured as BIGINT) as items_insured,
        claim_reference,
        insured_name,
        CAST(policy_start_date as DATE) as policy_start_date,
        CAST(date_of_loss as DATE) as date_of_loss,
        CAST(date_reported as DATE) as date_reported,
        claim_status,
        loss_type,
        paid_amount,
        reserve_amount,
        total_incurred,
        claim_propensity
        

        FROM 's3://property-insurance-examples/test-bordereaux-data/daily_bordereaux_merged_data/'
    )
FILEFORMAT = CSV FORMAT_OPTIONS('sep' = ',', 'header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('force' = 'true', 'mergeSchema' = 'true')