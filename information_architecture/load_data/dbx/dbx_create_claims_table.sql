

CREATE TABLE claims_workspace.bronze.bordereaux_data (
    policy_number        STRING,
    months_insured       INT,
    has_claims           BOOLEAN,
    items_insured        INT,
    claim_reference      STRING,
    insured_name         STRING,
    policy_start_date    TIMESTAMP,
    date_of_loss         TIMESTAMP,
    date_reported        TIMESTAMP,
    claim_status         STRING,
    loss_type            STRING,
    paid_amount          DOUBLE,
    reserve_amount       DOUBLE,
    total_incurred       DOUBLE,
    claim_propensity     DOUBLE
);



