import argparse

# Import functions
from extract.extract_ppp import extract_ppp_data
from extract.extract_naics import extract_naics_data
from extract.extract_gdp import extract_gdp_data
from clean.clean_naics import clean_naics_data
from clean.clean_ppp import clean_ppp_data
from clean.clean_gdp import clean_gdp_data
from transform.transform_ppp import transform_ppp_data
from transform.transform_naics import transform_naics_data
from transform.transform_gdp import transform_gdp_data
from transform.transform_dim_date import transform_dim_date
from transform.transform_dim_ppp import transform_dim_ppp_data


def run_stage(stage: str, dataset: str):
    if stage == "extract":
        if dataset == "ppp":
            extract_ppp_data()
        elif dataset == "naics":
            extract_naics_data()
        elif dataset == "gdp":
            extract_gdp_data()
        elif dataset == "all":
            extract_ppp_data()
            extract_naics_data()
            extract_gdp_data()
    elif stage == "clean":
        if dataset == "ppp":
            clean_ppp_data()
        elif dataset == "naics":
            clean_naics_data()
        elif dataset == "gdp":
            clean_gdp_data()
        elif dataset == "all":
            clean_ppp_data()
            clean_naics_data()
            clean_gdp_data()
    elif stage == "transform":
        if dataset == "ppp":
            #transform_ppp_data()
            transform_dim_ppp_data()
        elif dataset == "naics":
            transform_naics_data()
        elif dataset == "gdp":
            transform_gdp_data()
        elif dataset == "dim_date":
            transform_dim_date()
        elif dataset == "all":
            transform_ppp_data()
            transform_naics_data()
            transform_gdp_data()
            transform_dim_date()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline for PPP Loan Project")
    parser.add_argument("--stage", choices=["extract", "clean", "transform"], required=True, help="ETL stage to run")
    parser.add_argument("--dataset", choices=["ppp", "naics", "gdp", "dim_date", "all"], required=True, help="Dataset to process")

    args = parser.parse_args()
    print(f"\n[INFO] Running stage: {args.stage} on dataset: {args.dataset}")
    run_stage(args.stage, args.dataset)
    print(f"[SUCCESS] Completed {args.stage} for {args.dataset}")