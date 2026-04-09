from dotenv import load_dotenv
load_dotenv()

import requests
import random
import json
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import os

# ❌ NO random.seed(42) - we want fresh data each run

def fetch_products():
    """Fetch products from DummyJSON API"""
    url = "https://dummyjson.com/products?limit=100"
    response = requests.get(url)
    return response.json()['products']

def fetch_users():
    """Fetch users from DummyJSON API"""
    url = "https://dummyjson.com/users?limit=100"
    response = requests.get(url)
    return response.json()['users']

def generate_unique_cart_ids(count=10):
    """
    Generate unique random 5-6 digit cart IDs
    Ensures no duplicates within the batch
    """
    cart_ids = set()
    
    while len(cart_ids) < count:
        # Generate random 5-6 digit number (10000 to 999999)
        cart_id = random.randint(10000, 999999)
        cart_ids.add(cart_id)
    
    return list(cart_ids)

def generate_synthetic_carts(products, users, num_carts=10):
    """
    Generate completely NEW synthetic carts
    (Not using DummyJSON carts API)
    """
    synthetic_carts = []
    unique_cart_ids = generate_unique_cart_ids(num_carts)
    
    for cart_id in unique_cart_ids:
        # Random user
        user = random.choice(users)
        
        # Random number of products in cart (1-5 items)
        num_items = random.randint(1, 5)
        
        # Select random products
        cart_products = random.sample(products, num_items)
        
        cart = {
            'id': cart_id,  # ✅ Unique random ID
            'userId': user['id'],
            'products': [
                {
                    'id': prod['id'],
                    'quantity': random.randint(1, 3)
                }
                for prod in cart_products
            ],
            'totalProducts': len(cart_products),
            'totalQuantity': sum([random.randint(1, 3) for _ in cart_products])
        }
        
        synthetic_carts.append(cart)
    
    return synthetic_carts

def enrich_cart_data(carts, products, users):
    """
    Enrich cart data with business logic
    ✅ Uses the unique cart IDs we generated
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
        
        # Abandonment probability (fresh randomness!)
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
            # Use the unique cart_id (5-6 digits)
            "cart_id": cart['id'],  # Already unique from generate_unique_cart_ids()
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
            "event_timestamp": datetime.now().isoformat(),
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
        print(f"   Cart IDs: {[d['cart_id'] for d in data]}")
        
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
    
    # Step 1: Fetch reference data from API
    print("\n📡 Step 1: Fetching reference data from DummyJSON API...")
    products = fetch_products()
    users = fetch_users()
    print(f"   ✅ Fetched: {len(products)} products, {len(users)} users")
    
    # Step 2: Generate NEW synthetic carts (not from API)
    print("\n🎲 Step 2: Generating 10 NEW synthetic carts with unique IDs...")
    synthetic_carts = generate_synthetic_carts(products, users, num_carts=10)
    print(f"   ✅ Generated {len(synthetic_carts)} carts")
    print(f"   Cart IDs: {[cart['id'] for cart in synthetic_carts]}")
    
    # Step 3: Enrich data
    print("\n🔧 Step 3: Enriching cart data with business logic...")
    enriched_carts = enrich_cart_data(synthetic_carts, products, users)
    
    # Step 4: Show statistics
    df = pd.DataFrame(enriched_carts)
    abandonment_rate = (df['abandoned'].sum() / len(df) * 100)
    total_revenue = df['revenue'].sum()
    lost_revenue = df['potential_revenue'].sum()
    
    print(f"\n📊 Step 4: Batch Statistics:")
    print(f"   Total carts: {len(df)}")
    print(f"   Unique cart IDs: {df['cart_id'].nunique()}")
    print(f"   Abandonment rate: {abandonment_rate:.1f}%")
    print(f"   Revenue generated: ${total_revenue:,.2f}")
    print(f"   Revenue lost: ${lost_revenue:,.2f}")
    
    # Step 5: Upload to Azure
    print(f"\n☁️  Step 5: Uploading to Azure Blob Storage...")
    
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
        print(f"   Cart IDs in file: {[d['cart_id'] for d in enriched_carts]}")
    else:
        upload_to_azure(enriched_carts, connection_string, container_name)
    
    print(f"\n{'='*60}")
    print(f"✅ HOURLY JOB COMPLETED SUCCESSFULLY")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
