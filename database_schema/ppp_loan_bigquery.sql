CREATE TABLE ppploan.dim_borrower ( 
	borrower_id bignumeric NOT NULL  ,
	borrower_name string  ,
	borrower_address string  ,
	borrower_city string  ,
	borrower_state string  ,
	borrower_zip string  ,
	race string  ,
	ethnicity string  ,
	gender string  ,
	veteran boolean  ,
	franchise_name string  ,
	nonprofit boolean  ,
	jobs_reported int64  
 );

ALTER TABLE ppploan.dim_borrower ADD PRIMARY KEY ( borrower_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_business_age ( 
	business_age_id int64 NOT NULL  ,
	business_age_description string  
 );

ALTER TABLE ppploan.dim_business_age ADD PRIMARY KEY ( business_age_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_business_type ( 
	business_type_id int64 NOT NULL  ,
	business_type string  
 );

ALTER TABLE ppploan.dim_business_type ADD PRIMARY KEY ( business_type_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_date ( 
	date_id bignumeric NOT NULL  ,
	year_number int64  ,
	month_number int64  ,
	quarter_number int64  ,
	day_number int64  ,
	hour_number int64  ,
	date_iso_format datetime  ,
	day_name string  ,
	month_name string  ,
	week_of_month int64  ,
	week_of_year int64  
 );

ALTER TABLE ppploan.dim_date ADD PRIMARY KEY ( date_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_geography ( 
	geofips int64 NOT NULL  ,
	geo_name string  ,
	region string  ,
	project_county_name string  ,
	project_state string  
 );

ALTER TABLE ppploan.dim_geography ADD PRIMARY KEY ( geofips )  NOT ENFORCED;

CREATE TABLE ppploan.dim_loan_status ( 
	loan_status_id int64 NOT NULL  ,
	loan_status string  
 );

ALTER TABLE ppploan.dim_loan_status ADD PRIMARY KEY ( loan_status_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_naics ( 
	naics_code int64 NOT NULL  ,
	naics_title string  ,
	description string  
 );

ALTER TABLE ppploan.dim_naics ADD PRIMARY KEY ( naics_code )  NOT ENFORCED;

CREATE TABLE ppploan.dim_originating_lender ( 
	originating_lender_id bignumeric NOT NULL  ,
	originating_lender_location_id int64  ,
	originating_lender string  ,
	originating_lender_city string  ,
	originating_lender_state string  
 );

ALTER TABLE ppploan.dim_originating_lender ADD PRIMARY KEY ( originating_lender_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_processing_method ( 
	processing_method_id int64 NOT NULL  ,
	processing_method string  
 );

ALTER TABLE ppploan.dim_processing_method ADD PRIMARY KEY ( processing_method_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_sba_office ( 
	sba_office_code int64 NOT NULL  
 );

ALTER TABLE ppploan.dim_sba_office ADD PRIMARY KEY ( sba_office_code )  NOT ENFORCED;

CREATE TABLE ppploan.dim_servicing_lender ( 
	servicing_lender_id bignumeric NOT NULL  ,
	servicing_lender_location_id int64  ,
	servicing_lender_name string  ,
	servicing_lender_address string  ,
	servicing_lender_city string  ,
	servicing_lender_state string  ,
	servicing_lender_zip string  
 );

ALTER TABLE ppploan.dim_servicing_lender ADD PRIMARY KEY ( servicing_lender_id )  NOT ENFORCED;

CREATE TABLE ppploan.dim_term ( 
	term_id int64 NOT NULL  ,
	term_month int64  
 );

ALTER TABLE ppploan.dim_term ADD PRIMARY KEY ( term_id )  NOT ENFORCED;

CREATE TABLE ppploan.facts_gdp ( 
	facts_gdp_id int64 NOT NULL  ,
	year_id bignumeric NOT NULL  ,
	real_gdp numeric  ,
	chain_type_index_gdp numeric  ,
	current_dollar_gdp numeric  ,
	geofips int64 NOT NULL  
 );

ALTER TABLE ppploan.facts_gdp ADD PRIMARY KEY ( facts_gdp_id )  NOT ENFORCED;

CREATE TABLE ppploan.facts_ppp ( 
	facts_ppp_id bignumeric NOT NULL  ,
	loan_number bignumeric  ,
	naics_code int64 NOT NULL  ,
	geofips int64 NOT NULL  ,
	date_approved_id bignumeric NOT NULL  ,
	loan_status_date_id bignumeric NOT NULL  ,
	forgiveness_date_id bignumeric NOT NULL  ,
	borrower_id bignumeric NOT NULL  ,
	originating_lender_id bignumeric NOT NULL  ,
	servicing_lender_id bignumeric NOT NULL  ,
	term_id int64 NOT NULL  ,
	loan_status_id int64 NOT NULL  ,
	processing_method_id int64 NOT NULL  ,
	sba_office_code int64 NOT NULL  ,
	business_age_id int64 NOT NULL  ,
	business_type_id int64 NOT NULL  ,
	sba_guaranty_percentage numeric  ,
	initial_approval_amount numeric  ,
	current_approval_amount numeric  ,
	undisbursed_amount numeric  ,
	forgiveness_amount numeric  
 );

ALTER TABLE ppploan.facts_ppp ADD PRIMARY KEY ( facts_ppp_id )  NOT ENFORCED;

ALTER TABLE ppploan.facts_gdp ADD CONSTRAINT fk_facts_gdp_dim_geography FOREIGN KEY ( geofips ) REFERENCES ppploan.dim_geography( geofips ) NOT ENFORCED;

ALTER TABLE ppploan.facts_gdp ADD CONSTRAINT fk_facts_gdp_dim_date FOREIGN KEY ( year_id ) REFERENCES ppploan.dim_date( date_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_naics FOREIGN KEY ( naics_code ) REFERENCES ppploan.dim_naics( naics_code ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_geography FOREIGN KEY ( geofips ) REFERENCES ppploan.dim_geography( geofips ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_date FOREIGN KEY ( date_approved_id ) REFERENCES ppploan.dim_date( date_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_date_0 FOREIGN KEY ( loan_status_date_id ) REFERENCES ppploan.dim_date( date_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_date_1 FOREIGN KEY ( forgiveness_date_id ) REFERENCES ppploan.dim_date( date_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_borrower FOREIGN KEY ( borrower_id ) REFERENCES ppploan.dim_borrower( borrower_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_originating_lender FOREIGN KEY ( originating_lender_id ) REFERENCES ppploan.dim_originating_lender( originating_lender_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_servicing_lender FOREIGN KEY ( servicing_lender_id ) REFERENCES ppploan.dim_servicing_lender( servicing_lender_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_term FOREIGN KEY ( term_id ) REFERENCES ppploan.dim_term( term_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_loan_status FOREIGN KEY ( loan_status_id ) REFERENCES ppploan.dim_loan_status( loan_status_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_processing_method FOREIGN KEY ( processing_method_id ) REFERENCES ppploan.dim_processing_method( processing_method_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_sba_office FOREIGN KEY ( sba_office_code ) REFERENCES ppploan.dim_sba_office( sba_office_code ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_business_age FOREIGN KEY ( business_age_id ) REFERENCES ppploan.dim_business_age( business_age_id ) NOT ENFORCED;

ALTER TABLE ppploan.facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_business_type FOREIGN KEY ( business_type_id ) REFERENCES ppploan.dim_business_type( business_type_id ) NOT ENFORCED;

