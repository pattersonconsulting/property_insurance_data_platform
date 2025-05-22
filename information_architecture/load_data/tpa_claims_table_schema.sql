CREATE TABLE claims_data (
    policy_number        VARCHAR(20),
    months_insured       INTEGER,
    has_claims           BOOLEAN,
    items_insured        INTEGER,
    claim_reference      VARCHAR(20),
    insured_name         VARCHAR(255),
    policy_start_date    DATE,
    date_of_loss         DATE,
    date_reported        DATE,
    claim_status         VARCHAR(50),
    loss_type            VARCHAR(100),
    paid_amount          NUMERIC(12, 2),
    reserve_amount       NUMERIC(12, 2),
    total_incurred       NUMERIC(12, 2),
    claim_propensity     NUMERIC(3, 2)
);
