--Creating Tables for HCAHPS Dashboard--

CREATE TABLE IF NOT EXISTS "postgres"."Hospital_Data".hospital_beds
(
    provider_ccn INTEGER,
    hospital_name VARCHAR(255),
    fiscal_year_begin_date VARCHAR(10),
    fiscal_year_end_date VARCHAR(10),
    number_of_beds INTEGER
);

CREATE TABLE IF NOT EXISTS "postgres"."Hospital_Data".HCAHPS_data
(
    facility_id VARCHAR(10),
    facility_name VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    county_or_parish VARCHAR(50),
    telephone_number VARCHAR(20),
    hcahps_measure_id VARCHAR(255),
    hcahps_question VARCHAR(255),
    hcahps_answer_description VARCHAR(255),
    hcahps_answer_percent INTEGER,
    num_completed_surveys INTEGER,
    survey_response_rate_percent INTEGER,
    start_date VARCHAR(10),
    end_date VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS "postgres"."Hospital_Data".Tableau_File AS 
WITH hospital_beds_prep AS(
	SELECT LPAD(CAST(provider_ccn AS text),6,'0') AS provider_ccn,
		   hospital_name,
		   TO_DATE(fiscal_year_begin_date,'MM/DD/YYYY') AS fiscal_year_begin_date,
		   TO_DATE(fiscal_year_end_date,'MM/DD/YYYY') AS fiscal_year_end_date,
		   number_of_beds,
		   ROW_NUMBER() OVER (PARTITION BY provider_ccn ORDER BY TO_DATE(fiscal_year_end_date,'MM/DD/YYYY') DESC) AS nth_row
	FROM "postgres"."Hospital_Data".hospital_beds
)

SELECT LPAD(CAST(facility_id as text),6,'0') AS provider_ccn,
	   TO_DATE(start_date,'MM/DD/YYYY') AS start_date_converted,
	   TO_DATE(end_date,'MM/DD/YYYY') AS end_date_converted,
	   hcahps.*,
	   beds.number_of_beds,
	   beds.fiscal_year_begin_date AS beds_start_report_period,
	   beds.fiscal_year_end_date AS beds_end_report_period
FROM "postgres"."Hospital_Data".hcahps_data AS hcahps
LEFT JOIN hospital_beds_prep AS beds
  ON LPAD(CAST(facility_id AS text),6,'0') = beds.provider_ccn
  AND beds.nth_row = 1