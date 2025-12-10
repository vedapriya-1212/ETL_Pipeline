import os
import pandas as pd
 
# Purpose: Clean and transform Churn dataset
def transform_data(raw_path):
    # Ensure the path is relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
 
    df = pd.read_csv(raw_path)
 
    # --- cleaning data ---
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    cols = ['tenure', 'MonthlyCharges', 'TotalCharges']
    df[cols] = df[cols].fillna(df[cols].median())
    cat_cols = df.select_dtypes(include='object').columns
    df[cat_cols] = df[cat_cols].fillna('Unknown')
    df['tenure_group'] = pd.cut(
    df['tenure'],
    bins=[-1, 12, 36, 60, float('inf')],
    labels=['New', 'Regular', 'Loyal', 'Champion']
    )
    df['monthly_charge_segment'] = pd.cut(
    df['MonthlyCharges'],
    bins=[-1, 30, 70, float('inf')],
    labels=['Low', 'Medium', 'High']
    )
    df['has_internet_service'] = df['InternetService'].map({
    'DSL': 1,
    'Fiber optic': 1,
    'No': 0
    })
    df['is_multi_line_user'] = (df['MultipleLines'] == 'Yes').astype(int)
    df['contract_type_code'] = df['Contract'].map({
    'Month-to-month': 0,
    'One year': 1,
    'Two year': 2
    })
    df.drop(columns=['customerID', 'gender'], inplace=True, errors="ignore")
    df.drop(columns=['Dependents'], inplace=True, errors="ignore")
    df.drop(columns=['DeviceProtection'], inplace=True, errors="ignore")
    df.drop(columns=['MultipleLines'], inplace=True, errors="ignore")
    df.drop(columns=['PhoneService'], inplace=True, errors="ignore")
    df.drop(columns=['SeniorCitizen'], inplace=True, errors="ignore")
    df.drop(columns=['Partner'], inplace=True, errors="ignore")
    df.drop(columns=['PaperlessBilling'], inplace=True, errors="ignore")
    df.drop(columns=['StreamingTV'], inplace=True, errors="ignore")
    df.drop(columns=['StreamingMovies'], inplace=True, errors="ignore")
    df.drop(columns=['TechSupport'], inplace=True, errors="ignore")
    df.drop(columns=['OnlineSecurity'], inplace=True, errors="ignore")
    df.drop(columns=['OnlineBackup'], inplace=True, errors="ignore")
    

    df.columns = [
    c.strip()
     .replace('MonthlyCharges', 'monthly_charges')
     .replace('TotalCharges', 'total_charges')
     .replace('InternetService', 'internet_service')
     .replace('PaymentMethod', 'payment_method')
     .replace('Churn', 'churn')
     .lower()
    for c in df.columns
]
 
    # --- Save transformed data ---
    # name the staged file to match your loader (change loader if you prefer a different name)
    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path
 
 
if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)