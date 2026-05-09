from prefect import flow, task
import pandas as pd
import random
import time
from datetime import datetime

print("🚀 [SYSTEM]: Initializing the Market Data Pipeline..."
      )
@task(name="Extract market data", retries=2, retry_delay_seconds=5)
def extract_data():
    print("📥 [EXTRACT]: Simulating data extraction from market sources...")
    time.sleep(1) # Simulate network delay
    # stimulate API data
    raw_data = [
        {"asset": "Bitcoin", "price": random.uniform(50000, 65000), "status": "active"},
        {"asset": "Ethereum", "price": random.uniform(2000, 4000), "status": "active"},
        {"asset": "Gold", "price": random.uniform(1800, 2400), "status": "active"}
    ]
    
    if random.random() < 0.2: # 20% chance to simulate an API failure
        raise Exception("API request failed due to network error.")
    
    return raw_data
@task(name="Transform and clean data")
def transform_data(raw_data):
    print("⚙️ [TRANSFORM]: Cleaning data and calculating market trends...")
    df = pd.DataFrame(raw_data)
    # Rounding prices to 2 decimals for the CEO
    df['price'] = df['price'].round(2)
    # Adding a timestamp
    df['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

@task(name="Send Slack Alert")
def send_alert(df):
    print("🚀 [LOAD]: Pushing data to secure Vault...")
    highest_asset = df.loc[df['price'].idxmax()]
    
    print("\n=================================================")
    print("💬 [SLACK NOTIFICATION SENT]")
    print(f"👑 To: Gavi & Ishika (Executive Channel)")
    print(f"📈 Top Asset Today: {highest_asset['asset']} at ${highest_asset['price']}")
    print("✅ Pipeline Executed Successfully!")
    print("=================================================\n")

# --- THE FLOW (The CEO) ---

@flow(name="Daily_Market_Sync_Flow", log_prints=True)
def main_pipeline():
    # Calling the tasks in order. Prefect tracks everything automatically!
    raw_data = extract_data()
    clean_data = transform_data(raw_data)
    send_alert(clean_data)

# Execution & Scheduling (The Automator)
if __name__ == "__main__":
    # Cron "0 0 * * *" means: Run at Minute 0, Hour 0 (Midnight), Every day, Every month.
    main_pipeline.serve(
        name="Nightly_Executive_Sync",
        cron="0 0 * * *", 
        tags=["production", "market-data"]
    )