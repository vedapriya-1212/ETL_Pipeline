import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# -------------------------------
# Supabase client
# -------------------------------
def get_supabase_client():
    load_dotenv()
    return create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY")
    )

# -------------------------------
# Validation Logic
# -------------------------------
def validate_data(csv_path, table_name="churn_data"):
    print("\n🔎 STARTING DATA VALIDATION\n")

    # Load CSV
    df = pd.read_csv(csv_path)
    original_row_count = len(df)

    # 1. Missing values check   
    numeric_cols = ["tenure", "monthly_charges", "total_charges"]
    missing_check = df[numeric_cols].isna().sum()
 
    # 2. Unique row count   
    unique_rows = df.drop_duplicates().shape[0]

    # 3. Supabase row count
    supabase = get_supabase_client()
    response = supabase.table(table_name).select("id", count="exact").execute()
    supabase_row_count = response.count

    # 4. Segment existence
    tenure_groups = set(df["tenure_group"].dropna().unique())
    charge_segments = set(df["monthly_charge_segment"].dropna().unique())

    expected_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    expected_charge_segments = {"Low", "Medium", "High"}

    # 5. Contract code validation
    invalid_contract_codes = df.loc[
        ~df["contract_type_code"].isin([0, 1, 2]),
        "contract_type_code"
    ]

    # PRINT SUMMARY
    print("✅ Validation Summary")
    print("-" * 40)

    print("1️⃣ Missing Values Check:")
    for col, val in missing_check.items():
        print(f"   {col}: {val} missing")

    print("\n2️⃣ Row Count Check:")
    print(f"   Original CSV rows   : {original_row_count}")
    print(f"   Unique rows         : {unique_rows}")
    print(f"   Supabase table rows : {supabase_row_count}")

    print("\n3️⃣ Segment Validation:")
    print(f"   tenure_group OK     : {tenure_groups == expected_tenure_groups}")
    print(f"   monthly_segment OK  : {charge_segments == expected_charge_segments}")

    print("\n4️⃣ Contract Code Validation:")
    if invalid_contract_codes.empty:
        print("   ✅ Only {0,1,2} found")
    else:
        print("   ❌ Invalid values:", invalid_contract_codes.unique())
    

    print("\n✅ Validation complete.\n")

# Run script
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(BASE_DIR, "data", "staged", "churn_transformed.csv")
    validate_data(csv_path)

