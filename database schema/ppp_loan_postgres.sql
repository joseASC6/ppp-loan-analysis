CREATE SCHEMA IF NOT EXISTS "ppp_loan";

CREATE  TABLE "ppp_loan".dim_borrower ( 
	borrower_id          BIGINT  NOT NULL  ,
	borrower_name        VARCHAR(200)    ,
	borrower_address     VARCHAR(200)    ,
	borrower_city        VARCHAR(100)    ,
	borrower_state       VARCHAR(100)    ,
	borrower_zip         VARCHAR(100)    ,
	race                 VARCHAR(100)    ,
	ethnicity            VARCHAR(100)    ,
	gender               VARCHAR(100)    ,
	veteran              BOOLEAN    ,
	franchise_name       VARCHAR(200)    ,
	nonprofit            BOOLEAN    ,
	jobs_reported        INT    ,
	CONSTRAINT pk_dim_borrower PRIMARY KEY ( borrower_id )
 );

CREATE  TABLE "ppp_loan".dim_business_age ( 
	business_age_id      INT  NOT NULL  ,
	business_age_description VARCHAR(200)    ,
	CONSTRAINT pk_dim_business_age PRIMARY KEY ( business_age_id )
 );

CREATE  TABLE "ppp_loan".dim_business_type ( 
	business_type_id     INT  NOT NULL  ,
	business_type        VARCHAR(200)    ,
	CONSTRAINT pk_dim_business_type PRIMARY KEY ( business_type_id )
 );

CREATE  TABLE "ppp_loan".dim_date ( 
	date_id              BIGINT  NOT NULL  ,
	year_number          INT    ,
	month_number         INT    ,
	quarter_number       INT    ,
	day_number           INT    ,
	hour_number          INT    ,
	date_iso_format      timestamp    ,
	day_name             VARCHAR(100)    ,
	month_name           VARCHAR(100)    ,
	week_of_month        INT    ,
	week_of_year         INT    ,
	CONSTRAINT pk_dim_date PRIMARY KEY ( date_id )
 );

CREATE  TABLE "ppp_loan".dim_geography ( 
	geofips              INT  NOT NULL  ,
	geo_name             VARCHAR(100)    ,
	region               VARCHAR(50)    ,
	project_county_name  VARCHAR(200)    ,
	project_state        VARCHAR(100)    ,
	CONSTRAINT pk_dim_geography PRIMARY KEY ( geofips )
 );

CREATE  TABLE "ppp_loan".dim_loan_status ( 
	loan_status_id       INT  NOT NULL  ,
	loan_status          VARCHAR(100)    ,
	CONSTRAINT pk_dim_loan_status PRIMARY KEY ( loan_status_id )
 );

CREATE  TABLE "ppp_loan".dim_naics ( 
	naics_code           INT  NOT NULL  ,
	naics_title          VARCHAR(200)    ,
	description          TEXT    ,
	CONSTRAINT pk_naics_code PRIMARY KEY ( naics_code )
 );

CREATE  TABLE "ppp_loan".dim_originating_lender ( 
	originating_lender_id BIGINT  NOT NULL  ,
	originating_lender_location_id INT    ,
	originating_lender   VARCHAR(200)    ,
	originating_lender_city VARCHAR(200)    ,
	originating_lender_state VARCHAR(100)    ,
	CONSTRAINT pk_dim_originating_lender PRIMARY KEY ( originating_lender_id )
 );

CREATE  TABLE "ppp_loan".dim_processing_method ( 
	processing_method_id INT  NOT NULL  ,
	processing_method    VARCHAR(100)    ,
	CONSTRAINT pk_dim_processing_method PRIMARY KEY ( processing_method_id )
 );

CREATE  TABLE "ppp_loan".dim_sba_office ( 
	sba_office_code      INT  NOT NULL  ,
	CONSTRAINT pk_dim_sba_office PRIMARY KEY ( sba_office_code )
 );

CREATE  TABLE "ppp_loan".dim_servicing_lender ( 
	servicing_lender_id  BIGINT  NOT NULL  ,
	servicing_lender_location_id INT    ,
	servicing_lender_name VARCHAR(200)    ,
	servicing_lender_address VARCHAR(200)    ,
	servicing_lender_city VARCHAR(100)    ,
	servicing_lender_state VARCHAR(200)    ,
	servicing_lender_zip VARCHAR(200)    ,
	CONSTRAINT pk_dim_servicing_lender PRIMARY KEY ( servicing_lender_id )
 );

CREATE  TABLE "ppp_loan".dim_term ( 
	term_id              INT  NOT NULL  ,
	term_month           INT    ,
	CONSTRAINT pk_dim_term PRIMARY KEY ( term_id )
 );


CREATE  TABLE "ppp_loan".facts_gdp ( 
	facts_gdp_id         INT  NOT NULL  ,
	year_id              BIGINT  NOT NULL  ,
	real_gdp             DECIMAL    ,
	chain_type_index_gdp DECIMAL    ,
	current_dollar_gdp   DECIMAL    ,
	geofips              INT  NOT NULL  ,
	CONSTRAINT pk_facts_gdp PRIMARY KEY ( facts_gdp_id )
 );

CREATE  TABLE "ppp_loan".facts_ppp ( 
	facts_ppp_id         BIGINT  NOT NULL  ,
	loan_number          BIGINT    ,
	naics_code           INT  NOT NULL  ,
	geofips              INT  NOT NULL  ,
	date_approved_id     BIGINT  NOT NULL  ,
	loan_status_date_id  BIGINT  NOT NULL  ,
	forgiveness_date_id  BIGINT  NOT NULL  ,
	borrower_id          BIGINT  NOT NULL  ,
	originating_lender_id BIGINT  NOT NULL  ,
	servicing_lender_id  BIGINT  NOT NULL  ,
	term_id              INT  NOT NULL  ,
	loan_status_id       INT  NOT NULL  ,
	processing_method_id INT  NOT NULL  ,
	sba_office_code      INT  NOT NULL  ,
	business_age_id      INT  NOT NULL  ,
	business_type_id     INT  NOT NULL  ,
	sba_guaranty_percentage DECIMAL    ,
	initial_approval_amount DECIMAL    ,
	current_approval_amount DECIMAL    ,
	undisbursed_amount   DECIMAL    ,
	forgiveness_amount   DECIMAL    ,
	CONSTRAINT pk_facts_ppp PRIMARY KEY ( facts_ppp_id )
 );

ALTER TABLE "ppp_loan".facts_gdp ADD CONSTRAINT fk_facts_gdp_dim_geography FOREIGN KEY ( geofips ) REFERENCES "ppp_loan".dim_geography( geofips );

ALTER TABLE "ppp_loan".facts_gdp ADD CONSTRAINT fk_facts_gdp_dim_date FOREIGN KEY ( year_id ) REFERENCES "ppp_loan".dim_date( date_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_naics FOREIGN KEY ( naics_code ) REFERENCES "ppp_loan".dim_naics( naics_code );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_geography FOREIGN KEY ( geofips ) REFERENCES "ppp_loan".dim_geography( geofips );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_date FOREIGN KEY ( date_approved_id ) REFERENCES "ppp_loan".dim_date( date_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_date_0 FOREIGN KEY ( loan_status_date_id ) REFERENCES "ppp_loan".dim_date( date_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_date_1 FOREIGN KEY ( forgiveness_date_id ) REFERENCES "ppp_loan".dim_date( date_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_borrower FOREIGN KEY ( borrower_id ) REFERENCES "ppp_loan".dim_borrower( borrower_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_originating_lender FOREIGN KEY ( originating_lender_id ) REFERENCES "ppp_loan".dim_originating_lender( originating_lender_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_servicing_lender FOREIGN KEY ( servicing_lender_id ) REFERENCES "ppp_loan".dim_servicing_lender( servicing_lender_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_term FOREIGN KEY ( term_id ) REFERENCES "ppp_loan".dim_term( term_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_loan_status FOREIGN KEY ( loan_status_id ) REFERENCES "ppp_loan".dim_loan_status( loan_status_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_processing_method FOREIGN KEY ( processing_method_id ) REFERENCES "ppp_loan".dim_processing_method( processing_method_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_sba_office FOREIGN KEY ( sba_office_code ) REFERENCES "ppp_loan".dim_sba_office( sba_office_code );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_business_age FOREIGN KEY ( business_age_id ) REFERENCES "ppp_loan".dim_business_age( business_age_id );

ALTER TABLE "ppp_loan".facts_ppp ADD CONSTRAINT fk_facts_ppp_dim_business_type FOREIGN KEY ( business_type_id ) REFERENCES "ppp_loan".dim_business_type( business_type_id );
