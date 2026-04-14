"""
generate_sample_data.py
=======================
Generates a large realistic sales CSV dataset for ETL POC testing.
Produces ~1 million rows (~150 MB CSV) by default.

Usage:
    python generate_sample_data.py --rows 1000000 --output ./sample_sales.csv
    python generate_sample_data.py --rows 5000000 --output ./sample_sales_large.csv --upload --bucket <RAW_BUCKET>
"""

import csv
import random
import uuid
import argparse
import subprocess
from datetime import datetime, timedelta

# ── Reference data ────────────────────────────────────────────────────────────

REGIONS = {
    "North America": ["United States", "Canada", "Mexico"],
    "Europe":        ["United Kingdom", "Germany", "France", "Italy", "Spain", "Netherlands"],
    "Asia Pacific":  ["India", "Japan", "Australia", "Singapore", "South Korea"],
    "Latin America": ["Brazil", "Argentina", "Colombia", "Chile"],
    "Middle East":   ["UAE", "Saudi Arabia", "Israel"],
}

CATEGORIES = {
    "Electronics": {
        "Computers":    [("Laptop Pro 15", 1200), ("Gaming Laptop", 1800), ("Ultrabook X1", 999),
                         ("Workstation Z", 2500), ("Chromebook Air", 399)],
        "Smartphones":  [("PhoneX 14", 999), ("Galaxy S23", 849), ("Pixel 8", 699),
                         ("Budget Phone", 199), ("Flagship Plus", 1199)],
        "Accessories":  [("Wireless Earbuds", 149), ("USB-C Hub", 49), ("Mechanical Keyboard", 129),
                         ("Gaming Mouse", 79), ("4K Webcam", 89)],
    },
    "Clothing": {
        "Men":          [("Formal Shirt", 45), ("Denim Jeans", 65), ("Casual Tee", 25),
                         ("Wool Sweater", 89), ("Sports Shorts", 35)],
        "Women":        [("Floral Dress", 75), ("Blazer", 120), ("Yoga Pants", 55),
                         ("Summer Top", 30), ("Evening Gown", 250)],
        "Footwear":     [("Running Shoes", 110), ("Leather Boots", 180), ("Sandals", 45),
                         ("Casual Sneakers", 75), ("Formal Oxford", 145)],
    },
    "Home & Kitchen": {
        "Appliances":   [("Air Fryer", 89), ("Coffee Maker", 49), ("Instant Pot", 99),
                         ("Stand Mixer", 299), ("Toaster Oven", 65)],
        "Furniture":    [("Office Chair", 249), ("Standing Desk", 399), ("Bookshelf", 149),
                         ("Sofa Set", 899), ("Dining Table", 499)],
        "Bedding":      [("Memory Foam Mattress", 599), ("Duvet Set", 89), ("Pillow Pack", 45),
                         ("Bed Frame", 299), ("Electric Blanket", 69)],
    },
    "Sports & Fitness": {
        "Equipment":    [("Yoga Mat", 35), ("Resistance Bands", 25), ("Dumbbell Set", 149),
                         ("Treadmill", 799), ("Rowing Machine", 499)],
        "Outdoor":      [("Camping Tent", 199), ("Hiking Boots", 149), ("Bicycle", 599),
                         ("Kayak", 899), ("Sleeping Bag", 79)],
    },
    "Books & Media": {
        "Books":        [("Python Programming", 39), ("Data Science Handbook", 55),
                         ("Cloud Architecture", 49), ("Fiction Bestseller", 15), ("History Vol 1", 25)],
        "Software":     [("Office Suite", 149), ("Design Tool", 599), ("Security Suite", 49),
                         ("Video Editor", 299), ("Cloud Storage 1TB", 99)],
    },
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Crypto", "Gift Card"]
ORDER_STATUSES  = ["Delivered", "Delivered", "Delivered", "Shipped", "Processing",
                   "Cancelled", "Returned", "Pending"]   # weighted toward Delivered

START_DATE = datetime(2021, 1, 1)
END_DATE   = datetime(2024, 12, 31)


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_row(row_id: int) -> dict:
    region   = random.choice(list(REGIONS.keys()))
    country  = random.choice(REGIONS[region])
    category = random.choice(list(CATEGORIES.keys()))
    sub_cat  = random.choice(list(CATEGORIES[category].keys()))
    product_name, base_price = random.choice(CATEGORIES[category][sub_cat])

    # Add price variation ±20%
    unit_price = round(base_price * random.uniform(0.80, 1.20), 2)
    quantity   = random.choices([1, 2, 3, 5, 10], weights=[50, 25, 15, 7, 3])[0]
    discount   = round(random.choices(
        [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
        weights=[40, 20, 15, 10, 8, 5, 2]
    )[0], 2)

    sales_amount  = round(quantity * unit_price * (1 - discount), 2)
    profit_margin = random.uniform(0.05, 0.35)
    profit        = round(sales_amount * profit_margin, 2)
    shipping_cost = round(random.uniform(0, 25) if sales_amount < 100 else 0, 2)

    order_date = random_date(START_DATE, END_DATE)
    ship_days  = random.randint(1, 7)
    ship_date  = order_date + timedelta(days=ship_days)

    return {
        "order_id":       f"ORD-{uuid.uuid4().hex[:12].upper()}",
        "customer_id":    f"CUST-{random.randint(1000, 99999):05d}",
        "product_id":     f"PROD-{abs(hash(product_name)) % 10000:04d}",
        "product_name":   product_name,
        "category":       category,
        "sub_category":   sub_cat,
        "region":         region,
        "country":        country,
        "order_date":     order_date.strftime("%Y-%m-%d"),
        "ship_date":      ship_date.strftime("%Y-%m-%d"),
        "quantity":       quantity,
        "unit_price":     unit_price,
        "discount":       discount,
        "sales_amount":   sales_amount,
        "profit":         profit,
        "shipping_cost":  shipping_cost,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status":         random.choice(ORDER_STATUSES),
    }


def inject_dirty_rows(rows: list, pct: float = 0.02) -> list:
    """Inject ~2% dirty rows to test Job 1 cleaning logic."""
    dirty_count = int(len(rows) * pct)
    dirty_rows  = []
    for _ in range(dirty_count):
        row = generate_row(-1).copy()
        choice = random.randint(0, 4)
        if choice == 0:
            row["order_id"]    = ""          # missing required field
        elif choice == 1:
            row["customer_id"] = "  "        # whitespace only
        elif choice == 2:
            row["quantity"]    = "-5"        # invalid negative
        elif choice == 3:
            row["unit_price"]  = "abc"       # non-numeric
        elif choice == 4:
            row["order_date"]  = "not-a-date"
        dirty_rows.append(row)
    return rows + dirty_rows


def main():
    parser = argparse.ArgumentParser(description="Generate large sample CSV for ETL POC")
    parser.add_argument("--rows",   type=int,  default=1_000_000, help="Number of clean rows")
    parser.add_argument("--output", type=str,  default="sample_sales.csv")
    parser.add_argument("--upload", action="store_true", help="Upload to S3 after generating")
    parser.add_argument("--bucket", type=str,  default="", help="S3 bucket name for upload")
    parser.add_argument("--chunk",  type=int,  default=100_000, help="Write rows in chunks")
    args = parser.parse_args()

    print(f"Generating {args.rows:,} rows → {args.output}")

    fieldnames = [
        "order_id", "customer_id", "product_id", "product_name",
        "category", "sub_category", "region", "country",
        "order_date", "ship_date", "quantity", "unit_price",
        "discount", "sales_amount", "profit", "shipping_cost",
        "payment_method", "status",
    ]

    rows_written = 0
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while rows_written < args.rows:
            chunk_size = min(args.chunk, args.rows - rows_written)
            chunk = [generate_row(rows_written + i) for i in range(chunk_size)]

            # Inject dirty rows in first chunk only
            if rows_written == 0:
                chunk = inject_dirty_rows(chunk, pct=0.02)

            random.shuffle(chunk)
            writer.writerows(chunk)
            rows_written += chunk_size

            pct = rows_written / args.rows * 100
            print(f"  Progress: {rows_written:,} / {args.rows:,} ({pct:.1f}%)", end="\r")

    print(f"\nDone. File: {args.output}")

    # Check file size
    import os
    size_mb = os.path.getsize(args.output) / (1024 * 1024)
    print(f"File size: {size_mb:.1f} MB")

    if args.upload and args.bucket:
        from datetime import date
        today   = date.today().strftime("%Y-%m-%d")
        s3_path = f"s3://{args.bucket}/input/sales/dt={today}/sample_sales.csv"
        print(f"Uploading to {s3_path} …")
        result = subprocess.run(
            ["aws", "s3", "cp", args.output, s3_path, "--only-show-errors"],
            check=True
        )
        print(f"Upload complete → {s3_path}")


if __name__ == "__main__":
    random.seed(42)   # reproducible output
    main()
