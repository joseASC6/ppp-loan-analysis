# PPP_loan_analysis
## Analysis of the PPP Loans that were issued to Small Businesses during COVID-19

By Jose Garcia, Erika Chunzho and Gelbert Ramos

## Table of Contents:
1) Business Problem
    Context and Business Problem
    Business Requirements
    Functional Requirements
2) Business Impact
3) Business Persona
4) Data
5) Methods
    Information Architecture Diagram
    Data Architecture Diagram 
    Technical Architecture Diagram
    Data Pipelines/ ETL/ELT Diagram
    Dimensional Modeling Diagram
6) Data Tools
7) Interface

## 1. Business Problem

#### Context and Business Problem
During the COVID-19 pandemic, the U.S. government established the Paycheck Protection Program (PPP). The program provided loans to small businesses to help them maintain their workforce during the pandemic. Most loans were forgivable if used primarily for payroll and other specified business expenses like utilities.

The PPP was designed to provide rapid financial assistance to small businesses nationwide. However, this rapid deployment raised concerns about the program's effectiveness and equitable distribution of funds. The PPP faced several challenges, including eligibility issues, fraud, and disparities in loan distribution among different demographic groups and geographic areas.

### Business Requirements
#### The primary business problems this analysis aims to address are:
1. Measuring and Analyzing the Distribution of PPP Loans:
    * Calculate the total loan amounts disbursed by state, industry, and company.
2. Identifying Geographic Areas or Industries That May Have Been Underserved or Overserved:
    * Analyze the distribution of PPP loans across different states, counties, and zip codes to identify geographic disparities.
3. Assessing the Effectiveness and Impact of the PPP Loan Program:
    * Evaluate how well the PPP loans supported business survival and recovery during the COVID-19 pandemic.

#### Key Performance Indicators (KPIs): Establish metrics for business success
1. Loan-to-GDP Ratio: Measures the proportion of PPP loans relative to the economic output of counties and states.
    * Ratio to compare the total loan amount allocated vs GDP by county and states
    * (Total Loan Amount / GDP) * 100
2. Nice to have: Industry Impact Ratio: Compares the proportion of loans given to an industry relative to its economic contribution.
    * (Loan Amount to Industry / Total Loan Amount) / (Industry GDP / Total GDP)


### Functional Requirements 
#### Business Intelligence Use Cases:
1. Analyze PPP loan disbursements by company, industry, geography
2. Monitor loan amounts and number of businesses assisted by lender
3. Report on top lenders, industries, and geographic areas based on loan portfolios
4. Analyze the correlation between PPP loans and economic indicators like GDP
 
##### Requirements:
    1. Must have
        a. Total loan by companies
        b. Total loan by State
        c. Total Business by States
        d. Top 10 Lenders by State
        e. Total Loan by Lenders
        f. Classification by NAICS Code
        g. Total amount per industry, PPP, then PPS
        h. Aggregate data based on GDP for the past four years, or COVID-19 per zip code

    2. Should have 
        a. Loan forgiveness amount and rate by state, industry, and lender
        b. Track loan approvals and disbursements over time to identify trends and patterns.
        c. Compare loan distribution between rural and urban areas.

    3. Nice to have
        a. Analyze loan distribution based on Race, Ethnicity, Gender, and Veteran status fields to assess equity in loan allocation.
        b. Calculate and compare the average processing time and approval rates for different lenders.

    4. Forget about it (which could also be future work)
        a. Long-term business performance tracking: Would need access to    ongoing financial data for loan recipients
        Integration with real-time economic indicators
        b. Fraud detection analysis

## 2. Business Impact
Analyzing PPP loans issued to small businesses during COVID-19 significantly impacts stakeholders, including government agencies, financial institutions, policymakers, and small business owners. The project's impacts include its risks, costs, and benefits.  

This project's risks include data quality issues and privacy concerns. Some PPP loan data is incomplete, missing, or inconsistent from the rest, which could lead to a flawed or inaccurate conclusion. In addition, the PPP data contains sensitive information about the business owners. This project must ensure that it protects and secures this information.

The cost of building the BI solutions is estimated at $40,000. The project is estimated to take 480 hours, and each team member will be paid $50 per hour, resulting in total labor costs of $32,000. In addition, the project includes $4,000 for software and tools, $2,000 for data acquisitions, and $2,000 for the project documentation.

Lastly, project benefits include data-driven decision-making for future economic relief programs, improved policy-making, and measuring a loan’s impact on a business or industry. Ideally, this project should allow governments to reallocate PPP loan funds based on the county and GDP to ensure the loans are distributed equitably. The data from the PPP is a decisive tool for future businesses. It can help them understand how this program can contribute to their growth. Moreover, this data is invaluable for policymakers and analysts, as it can guide them in making informed decisions for future economic relief programs. It can also provide insights into whether the loans have positively or negatively impacted businesses and industries, further empowering decision-makers.

## 3. Business Persona 
The people who will use this system are the financial institutions that issued the PPP loans, government agencies overseeing the SBA, policymakers studying the impact of pandemic relief, business and financial analysts interested in analyzing PPP Loans, government and state officials, and the client, Professor Jefferson Bien-Aimé.

## 4. Data
For this project, we will use three main sources of data: the U.S. Small Business Administration provides public records of PPP loan data, the Bureau of Economic Analysis contains GDP data for each county, and the U.S. Census Bureau provides data on the North American Industry Classification System (NAICS). 

The PPP loan data has 13 CSV files, each containing more than 900,000 records. Each record contains information on a specific loan provided by the SBA offices. The NAICS data is an XLSX file, (which will be converted to CSV) with 2,126 records containing the industry codes, names, and descriptions.  This data contains duplicate records with the same industry containing multiple codes. The GDP has 1 CSV file with 9,535 records of the GDP of a specific state, county, or area. Each area is represented by three records in the table: real GDP, chain-type quantity indexes for real GDP, and current-dollar GDP. 

## 5. Methods
In this section, we will highlight some of the key tools and elements of our tech stack that we will be using for this project. As we learn and build upon our skills during this program, we hope to add additional tools to this section and improve upon some of our initial assumptions. Please include the following : 

#### 1. Information Architecture Diagram

<!-- ![alt text](datamodels/data_architecture_diagram.png) -->
<img src="datamodels/information_architecture_diagram.png" width="400">

#### 2. Data Architecture Diagram

<!-- ![alt text](datamodels/data_architecture_diagram.png) -->
<img src="datamodels/data_architecture_diagram.png" width="400">

#### 3. Dimensional Modeling Diagram

<!-- ![alt text](datamodels/Logical_Dimensional_Modeling_Diagram.png) -->
<img src="datamodels/Logical_Dimensional_Modeling_Diagram.png" width="400">

#### 4. Technical Architecture Diagram

<img src="datamodels/technical_architecture_diagram.jpeg" width="400">

#### 5. Data Pipelines/ ETL/ELT Diagram

<img src="datamodels/data_pipelines_etl_diagram.jpeg" width="400">

## 6. Data Tools
#### Data Storage: What are the data storages that you are going to use? 
For data storage, we will utilize cloud Storage, which includes Microsoft Azure. The storage we have is called stppp.

#### Data Processing: How are you going to process/ingest the data? 
We will use the ETL (Extracting, Transforming, and loading) process. 
The data was collected raw, so it needs to be cleaned, and duplicates or sections that do not need to be analyzed will be removed. Then, the data will be converted into data types. All the converted datasets would come together and be stored in a database, allowing the user to efficiently obtain them when they are needed. 

## 7. Interface
Visuals created using Tableau

![Loan-to-GDP Ratio by State and County](https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/d08e57bc-0613-4c51-b672-d43d290d9ae2)
![Loan-to-GDP Ratio by State](https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/f16050b7-c27e-4f18-862c-3bba1eb7a613)
![Top 10 Lenders by State](https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/ee1ba20a-3a6a-458e-841c-6c89f7e01fb4)
![Total Amount Per Industry PPP then PPS](https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/f96a46da-65db-4b82-b0e9-8cf3b1cbc779)
![Total Loan and Business by State](https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/915e2f77-4c88-419c-bc46-d08fe2bd179c)
![Total Loan by Borrower Companies](https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/9eb857ce-2530-4564-92c0-94eae7d448b9)
![Distribution of PPP Loans by State ](https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/c9b2cff5-b91d-4c34-8d16-651d85d720fa)
<img width="812" alt="Classifcation by NAICS" src="https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/249e2cab-eb15-4cf3-9c75-c41abe31ee41">
<img width="779" alt="Total Loan by Company" src="https://github.com/joseASC6/PPP_loan_analysis/assets/52749491/201c55f8-9994-483d-a10b-301ece584de0">







