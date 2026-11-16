-- SCD Type 1 & Type 2 Implementation in Hive


-- Step 1: Create a temporary work table
DROP TABLE IF EXISTS dim_user_new;
CREATE TABLE dim_user_new LIKE dim_user_production;



-- Step 2: Preserve records from production that are NOT in staging
INSERT INTO dim_user_new
SELECT p.*
FROM dim_user_production p
LEFT JOIN dim_user_staging s ON p.login = s.login
WHERE s.login IS NULL;

-- Step 3: Handle SCD Type 1 changes for Historical (Inactive) records
INSERT INTO TABLE dim_user_new
SELECT p.dim_user_id,
  p.login,
  p.premium_user,
  p.address,
  p.phone,
  s.name,
  s.surname,
  s.year_of_birth,
  p.scd_version,
  p.scd_start_date,
  p.scd_end_date,
  p.scd_active
FROM dim_user_production p
JOIN dim_user_staging s ON p.login = s.login
AND p.scd_active = false; 







