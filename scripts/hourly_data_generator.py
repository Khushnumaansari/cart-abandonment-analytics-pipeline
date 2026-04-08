from dotenv import load_dotenv
load_dotenv()

# hourly_data_generator.py
import requests
import random
import json
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import os

# ❌ NO random.seed(42) here - we want fresh data each run

def fetch_products():
    """Fetch products from DummyJSON API"""
    url = "https://dummyjson.com/products?limit=100"
    response = requests.get(url)
    return response.json()['products']

def fetch_carts():
    """Fetch carts from DummyJSON API"""
    url = "https://dummyjson.com/carts?limit=100"
    response = requests.get(url)
    return response.json()['carts']

def fetch_users():
    """Fetch users from DummyJSON API"""
    url = "https://dummyjson.com/users?limit=100"
    response = requests.get(url)
    return response.json()['users']

def enrich_cart_data(carts, products, users):
    """
    YOUR EXACT SAME ENRICHMENT LOGIC FROM JUPYTER
    Copy-paste your enrichment function here
    """
    enriched_carts = []
    
    for cart in carts:
        # Calculate cart value
        cart_total = 0
        product_count = 0
        product_categories = []
        
        for item in cart['products']:
            # Find product details
            product = next((p for p in products if p['id'] == item['id']), None)
            if product:
                # Calculate discounted price
                discount_amount = product['price'] * (product['discountPercentage'] / 100)
                final_price = product['price'] - discount_amount
                cart_total += final_price * item['quantity']
                product_count += item['quantity']
                product_categories.append(product['category'])
        
        # Find user details
        user = next((u for u in users if u['id'] == cart['userId']), None)
        
        # Abandonment probability (NO SEED - fresh randomness!)
        abandonment_probability = 0.3
        
        if cart_total > 500:
            abandonment_probability += 0.2
        if product_count > 5:
            abandonment_probability += 0.15
        
        is_abandoned = random.random() < abandonment_probability
        
        session_duration = random.randint(2, 45) if not is_abandoned else random.randint(1, 10)
        device_type = random.choice(['Desktop', 'Mobile', 'Tablet'])
        
        if device_type == 'Mobile':
            abandonment_probability += 0.15
            is_abandoned = random.random() < abandonment_probability
        
        abandonment_reasons = [
            "High shipping cost",
            "Just browsing",
            "Found better price elsewhere",
            "Checkout too complicated",
            "Wanted to compare options",
            "Unexpected costs at checkout",
            "Required to create account",
            "Payment security concerns"
        ]
        
        enriched_cart = {
            "cart_id": cart['id'],
            "user_id": cart['userId'],
            "total_products": cart['totalProducts'],
            "total_quantity": cart['totalQuantity'],
            "cart_value": round(cart_total, 2),
            "product_count": product_count,
            "avg_product_price": round(cart_total / product_count, 2) if product_count > 0 else 0,
            "user_name": f"{user['firstName']} {user['lastName']}" if user else "Unknown",
            "user_age": user['age'] if user else None,
            "user_gender": user['gender'] if user else None,
            "device_type": device_type,
            "session_duration_minutes": session_duration,
            "timestamp": datetime.now().isoformat(),
            "batch_id": datetime.now().strftime("%Y%m%d_%H%M"),
            "abandoned": is_abandoned,
            "abandonment_reason": random.choice(abandonment_reasons) if is_abandoned else None,
            "cart_size_category": (
                "Small" if cart_total < 100 else
                "Medium" if cart_total < 300 else
                "Large" if cart_total < 600 else
                "Extra Large"
            ),
            "primary_category": max(set(product_categories), key=product_categories.count) if product_categories else "Unknown",
            "revenue": 0 if is_abandoned else round(cart_total, 2),
            "potential_revenue": round(cart_total, 2) if is_abandoned else 0
        }
        
        enriched_carts.append(enriched_cart)
    
    return enriched_carts

def upload_to_azure(data, connection_string, container_name):
    """Upload JSON to Azure Blob Storage"""
    try:
        # Connect to Azure
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Create unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"enriched_carts_{timestamp}.json"
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Upload
        json_data = json.dumps(data, indent=2)
        blob_client.upload_blob(json_data, overwrite=True)
        
        print(f"✅ Uploaded {blob_name} to Azure Blob Storage")
        print(f"   Container: {container_name}")
        print(f"   Size: {len(data)} carts")
        
        return blob_name
        
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        raise

def main():
    """Main function - runs every hour"""
    print(f"\n{'='*60}")
    print(f"🕐 HOURLY DATA GENERATION STARTED")
    print(f"{'='*60}")
    print(f"Timestamp: {datetime.now()}")
    
    # Step 1: Fetch data from API
    print("\n📡 Step 1: Fetching data from DummyJSON API...")
    products = fetch_products()
    carts = fetch_carts()
    users = fetch_users()
    print(f"   ✅ Fetched: {len(products)} products, {len(carts)} carts, {len(users)} users")
    
    # Step 2: Enrich data
    print("\n🔧 Step 2: Enriching cart data with business logic...")
    enriched_carts = enrich_cart_data(carts, products, users)
    
    # Step 3: Show statistics
    df = pd.DataFrame(enriched_carts)
    abandonment_rate = (df['abandoned'].sum() / len(df) * 100)
    total_revenue = df['revenue'].sum()
    lost_revenue = df['potential_revenue'].sum()
    
    print(f"\n📊 Step 3: Batch Statistics:")
    print(f"   Total carts: {len(df)}")
    print(f"   Abandonment rate: {abandonment_rate:.1f}%")
    print(f"   Revenue generated: ${total_revenue:,.2f}")
    print(f"   Revenue lost: ${lost_revenue:,.2f}")
    
    # Step 4: Upload to Azure
    print(f"\n☁️  Step 4: Uploading to Azure Blob Storage...")
    
    # Get connection string from environment variable
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = "cart-data"  # Your container name
    
    if not connection_string:
        print("⚠️  WARNING: AZURE_STORAGE_CONNECTION_STRING not found!")
        print("   Saving locally instead...")
        
        # Save locally if no Azure connection
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'enriched_carts_{timestamp}.json'
        with open(filename, 'w') as f:
            json.dump(enriched_carts, f, indent=2)
        print(f"   ✅ Saved locally: {filename}")
    else:
        upload_to_azure(enriched_carts, connection_string, container_name)
    
    print(f"\n{'='*60}")
    print(f"✅ HOURLY JOB COMPLETED SUCCESSFULLY")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()