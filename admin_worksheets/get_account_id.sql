-- The account_id is needed to properly setup the sf-git
SHOW REGIONS;
WITH
    SF_REGIONS AS (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))),
    INFOS AS (SELECT CURRENT_REGION() AS CR, CURRENT_ACCOUNT() AS CA)
SELECT CONCAT(
        LOWER(INFOS.CA),
        '.',
        SF_REGIONS."region",
        '.',
        SF_REGIONS."cloud"
    ) AS account_id
FROM INFOS LEFT JOIN SF_REGIONS ON INFOS.CR = SF_REGIONS."snowflake_region";

-- Unfortunately, the region is not always the same in the SHOW REGIONS ouput.
-- Please check and adapt the format comforming to the official documentation.
-- For example, eastus2 for Azure should actually be east-us-2.
-- https://docs.snowflake.com/en/user-guide/admin-account-identifier#non-vps-account-locator-formats-by-cloud-platform-and-region