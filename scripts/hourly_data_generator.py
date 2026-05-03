"""
Hourly Cart Data Generator
--------------------------
Generates a fresh batch of synthetic e-commerce cart events using DummyJSON
reference data, enriches the records with business fields, and uploads the
result as JSON to Azure Blob Storage.

Required environment variables in .env:
    AZURE_STORAGE_CONNECTION_STRING=your_connection_string
    AZURE_CONTAINER_NAME=cart-data

Run:
    python scripts/hourly_data_generator.py
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

PRODUCTS_URL = "https://dummyjson.com/products?limit=100"
USERS_URL = "https://dummyjson.com/users?limit=100"
DEFAULT_CONTAINER_NAME = "cart-data"
DEFAULT_BATCH_SIZE = 10


def fetch_json(url: str) -> Dict[str, Any]:
    """Fetch JSON from an API with basic timeout and error handling."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_products() -> List[Dict[str, Any]]:
    """Fetch product reference data from DummyJSON."""
    data = fetch_json(PRODUCTS_URL)
    return data.get("products", [])


def fetch_users() -> List[Dict[str, Any]]:
    """Fetch user reference data from DummyJSON."""
    data = fetch_json(USERS_URL)
    return data.get("users", [])


def generate_unique_cart_ids(count: int = DEFAULT_BATCH_SIZE) -> List[int]:
    """
    Generate unique integer cart IDs for the current batch.

    Note: In a production system, event_id or UUID would be better than cart_id
    as the primary key. This keeps your current SQL schema compatible because
    cart_id is INT.
    """
    cart_ids: set[int] = set()

    while len(cart_ids) < count:
        cart_ids.add(random.randint(100000000, 999999999))

    return list(cart_ids)


def generate_synthetic_carts(
    products: List[Dict[str, Any]],
    users: List[Dict[str, Any]],
    num_carts: int = DEFAULT_BATCH_SIZE,
) -> List[Dict[str, Any]]:
    """Generate a fresh batch of synthetic cart records."""
    if not products:
        raise ValueError("No products available from API.")
    if not users:
        raise ValueError("No users available from API.")

    synthetic_carts: List[Dict[str, Any]] = []
    unique_cart_ids = generate_unique_cart_ids(num_carts)

    for cart_id in unique_cart_ids:
        user = random.choice(users)
        num_items = random.randint(1, 5)
        cart_products = random.sample(products, k=min(num_items, len(products)))

        # IMPORTANT FIX:
        # Create product quantities once, then calculate total_quantity from
        # those exact quantities. Your older version generated quantities twice,
        # so totalQuantity could be different from the product-level quantities.
        cart_items = [
            {
                "id": product["id"],
                "quantity": random.randint(1, 3),
            }
            for product in cart_products
        ]

        synthetic_carts.append(
            {
                "id": cart_id,
                "userId": user["id"],
                "products": cart_items,
                "totalProducts": len(cart_items),
                "totalQuantity": sum(item["quantity"] for item in cart_items),
            }
        )

    return synthetic_carts


def calculate_discounted_price(product: Dict[str, Any]) -> float:
    """Calculate product price after discount."""
    price = float(product.get("price", 0) or 0)
    discount_pct = float(product.get("discountPercentage", 0) or 0)
    return price - (price * discount_pct / 100)


def assign_cart_size(cart_total: float) -> str:
    """Assign cart size category based on cart value."""
    if cart_total < 100:
        return "Small"
    if cart_total < 300:
        return "Medium"
    if cart_total < 600:
        return "Large"
    return "Extra Large"


def enrich_cart_data(
    carts: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    users: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Enrich raw cart records with user, product, revenue, and abandonment logic."""
    product_lookup = {product["id"]: product for product in products}
    user_lookup = {user["id"]: user for user in users}
    enriched_carts: List[Dict[str, Any]] = []

    abandonment_reasons = [
        "High shipping cost",
        "Just browsing",
        "Found better price elsewhere",
        "Checkout too complicated",
        "Wanted to compare options",
        "Unexpected costs at checkout",
        "Required to create account",
        "Payment security concerns",
    ]

    batch_timestamp = datetime.now().replace(microsecond=0).isoformat()

    for cart in carts:
        cart_total = 0.0
        product_count = 0
        product_categories: List[str] = []

        for item in cart["products"]:
            product = product_lookup.get(item["id"])
            if not product:
                continue

            quantity = int(item.get("quantity", 0) or 0)
            final_price = calculate_discounted_price(product)
            cart_total += final_price * quantity
            product_count += quantity
            product_categories.append(product.get("category", "Unknown"))

        user = user_lookup.get(cart["userId"])
        device_type = random.choice(["Desktop", "Mobile", "Tablet"])

        # Business-rule abandonment simulation.
        abandonment_probability = 0.30
        if cart_total > 500:
            abandonment_probability += 0.20
        if product_count > 5:
            abandonment_probability += 0.15
        if device_type == "Mobile":
            abandonment_probability += 0.15

        abandonment_probability = min(abandonment_probability, 0.95)
        is_abandoned = random.random() < abandonment_probability

        session_duration = (
            random.randint(1, 10) if is_abandoned else random.randint(8, 45)
        )

        primary_category = (
            max(set(product_categories), key=product_categories.count)
            if product_categories
            else "Unknown"
        )

        enriched_carts.append(
            {
                "cart_id": cart["id"],
                "user_id": cart["userId"],
                "total_products": int(cart["totalProducts"]),
                "total_quantity": int(cart["totalQuantity"]),
                "cart_value": round(cart_total, 2),
                "product_count": int(product_count),
                "avg_product_price": round(cart_total / product_count, 2) if product_count > 0 else 0,
                "user_name": f"{user['firstName']} {user['lastName']}" if user else "Unknown",
                "user_age": user.get("age") if user else None,
                "user_gender": user.get("gender") if user else None,
                "device_type": device_type,
                "session_duration_minutes": int(session_duration),

                # IMPORTANT FIX:
                # Use event_timestamp everywhere: Python JSON, ADF mapping,
                # SQL staging/main tables, SQL analytics refresh, and Power BI.
                "event_timestamp": batch_timestamp,

                "abandoned": bool(is_abandoned),
                "abandonment_reason": random.choice(abandonment_reasons) if is_abandoned else None,
                "cart_size_category": assign_cart_size(cart_total),
                "primary_category": primary_category,
                "revenue": 0 if is_abandoned else round(cart_total, 2),
                "potential_revenue": round(cart_total, 2) if is_abandoned else 0,
            }
        )

    return enriched_carts


def upload_to_azure(
    data: List[Dict[str, Any]],
    connection_string: str,
    container_name: str,
) -> str:
    """Upload enriched JSON data to Azure Blob Storage."""
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Create container if it does not exist. If it exists, ignore the error.
    try:
        container_client.create_container()
    except Exception:
        pass

    file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"enriched_carts_{file_timestamp}.json"

    json_data = json.dumps(data, indent=2)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json_data, overwrite=True)

    return blob_name


def save_locally(data: List[Dict[str, Any]]) -> str:
    """Save JSON locally when Azure credentials are not available."""
    file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"enriched_carts_{file_timestamp}.json"
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)
    return filename


def print_batch_summary(enriched_carts: List[Dict[str, Any]]) -> None:
    """Print quick statistics for the generated batch."""
    df = pd.DataFrame(enriched_carts)
    abandonment_rate = df["abandoned"].mean() * 100 if len(df) else 0
    total_revenue = df["revenue"].sum() if len(df) else 0
    lost_revenue = df["potential_revenue"].sum() if len(df) else 0

    print("\nBatch Summary")
    print("-" * 60)
    print(f"Total carts: {len(df)}")
    print(f"Unique cart IDs: {df['cart_id'].nunique() if len(df) else 0}")
    print(f"Abandonment rate: {abandonment_rate:.1f}%")
    print(f"Revenue generated: ${total_revenue:,.2f}")
    print(f"Potential lost revenue: ${lost_revenue:,.2f}")
    print(f"Cart IDs: {df['cart_id'].tolist() if len(df) else []}")


def main() -> None:
    """Run one batch generation and upload cycle."""
    print("\n" + "=" * 70)
    print("HOURLY CART DATA GENERATION STARTED")
    print("=" * 70)
    print(f"Run time: {datetime.now().replace(microsecond=0).isoformat()}")

    print("\nStep 1: Fetching reference data from DummyJSON API...")
    products = fetch_products()
    users = fetch_users()
    print(f"Fetched {len(products)} products and {len(users)} users.")

    print("\nStep 2: Generating fresh synthetic cart events...")
    synthetic_carts = generate_synthetic_carts(products, users, DEFAULT_BATCH_SIZE)
    print(f"Generated {len(synthetic_carts)} cart events.")

    print("\nStep 3: Enriching carts with business logic...")
    enriched_carts = enrich_cart_data(synthetic_carts, products, users)
    print_batch_summary(enriched_carts)

    print("\nStep 4: Uploading JSON batch...")
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME", DEFAULT_CONTAINER_NAME)

    if connection_string:
        blob_name = upload_to_azure(enriched_carts, connection_string, container_name)
        print(f"Uploaded to Azure Blob Storage: {container_name}/{blob_name}")
    else:
        filename = save_locally(enriched_carts)
        print("AZURE_STORAGE_CONNECTION_STRING not found.")
        print(f"Saved locally instead: {filename}")

    print("\n" + "=" * 70)
    print("HOURLY CART DATA GENERATION COMPLETED SUCCESSFULLY")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
