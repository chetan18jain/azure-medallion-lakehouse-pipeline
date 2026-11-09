"""Generate small raw CSV extracts (orders, customers, products).

Local stand-in for files landing in an ADLS Gen2 Bronze container.
Run:  python src/generate_sample_data.py --out data/raw
"""
import argparse, csv, os, random
from datetime import date, timedelta

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Sana", "Vikram", "Isha"]
LAST = ["Sharma", "Patel", "Reddy", "Iyer", "Khan", "Das", "Nair", "Singh"]
CITIES = ["Hyderabad", "Bengaluru", "Pune", "Mumbai", "Chennai"]
PRODUCTS = [
    ("P100", "Wireless Mouse", "Accessories", 1299.0),
    ("P101", "Mechanical Keyboard", "Accessories", 3499.0),
    ("P102", "USB-C Hub", "Accessories", 2199.0),
    ("P103", "Noise-Cancelling Headphones", "Audio", 5999.0),
    ("P104", "27-inch Monitor", "Displays", 15999.0),
    ("P105", "Laptop Stand", "Accessories", 1899.0),
]

def write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--orders", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    rng = random.Random(args.seed)

    customers = []
    for i in range(1, 31):
        customers.append([f"C{i:03d}", f"{rng.choice(FIRST)} {rng.choice(LAST)}",
                          rng.choice(CITIES), "IN"])
    write_csv(os.path.join(args.out, "customers.csv"),
              ["customer_id", "customer_name", "city", "country"], customers)

    write_csv(os.path.join(args.out, "products.csv"),
              ["product_id", "product_name", "category", "list_price"], PRODUCTS)

    start = date(2026, 1, 1)
    orders = []
    for i in range(1, args.orders + 1):
        product = rng.choice(PRODUCTS)
        qty = rng.randint(1, 4)
        day = start + timedelta(days=rng.randint(0, 29))
        orders.append([f"O{i:05d}", f"C{rng.randint(1, 30):03d}", product[0],
                       qty, round(product[3] * qty, 2), day.isoformat(),
                       rng.choice(["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED"])])
    write_csv(os.path.join(args.out, "orders.csv"),
              ["order_id", "customer_id", "product_id", "quantity",
               "amount", "event_date", "status"], orders)
    print(f"Wrote sample data to {args.out}")

if __name__ == "__main__":
    main()
