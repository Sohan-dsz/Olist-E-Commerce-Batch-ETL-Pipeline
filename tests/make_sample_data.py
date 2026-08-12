"""
Generates small CSVs with the exact column names/dtypes of the real Olist dataset
so the pipeline can be unit-tested without downloading the full ~120MB Kaggle dataset.
Column layouts taken from the public Olist schema docs.
"""
import csv
import random
from datetime import datetime, timedelta

random.seed(42)
OUT = "data/sample"

states = ["SP", "RJ", "MG", "BA", "RS", "PR"]
categories = ["beleza_saude", "informatica_acessorios", "moveis_decoracao", "esporte_lazer", "cama_mesa_banho"]

n_customers = 200
n_orders = 500

customers = []
for i in range(n_customers):
    customers.append({
        "customer_id": f"cust_{i}",
        "customer_unique_id": f"uniq_{i}",
        "customer_zip_code_prefix": f"{random.randint(10000,99999)}",
        "customer_city": "sao paulo" if i % 3 == 0 else "rio de janeiro",
        "customer_state": random.choice(states),
    })

orders = []
base_date = datetime(2017, 1, 1)
for i in range(n_orders):
    purchase = base_date + timedelta(days=random.randint(0, 700), hours=random.randint(0,23))
    # 5% malformed/missing delivered date, 3% duplicated order id (simulate real dupes)
    approved = purchase + timedelta(hours=random.randint(1, 48))
    delivered_carrier = purchase + timedelta(days=random.randint(1, 5))
    delivered_customer = purchase + timedelta(days=random.randint(5, 20))
    status = "delivered"
    if random.random() < 0.05:
        delivered_customer = None
        status = "shipped"
    if random.random() < 0.02:
        status = "canceled"
        delivered_customer = None

    orders.append({
        "order_id": f"order_{i}",
        "customer_id": f"cust_{random.randint(0, n_customers-1)}",
        "order_status": status,
        "order_purchase_timestamp": purchase.strftime("%Y-%m-%d %H:%M:%S"),
        "order_approved_at": approved.strftime("%Y-%m-%d %H:%M:%S"),
        "order_delivered_carrier_date": delivered_carrier.strftime("%Y-%m-%d %H:%M:%S"),
        "order_delivered_customer_date": delivered_customer.strftime("%Y-%m-%d %H:%M:%S") if delivered_customer else "",
        "order_estimated_delivery_date": (purchase + timedelta(days=15)).strftime("%Y-%m-%d %H:%M:%S"),
    })

# inject a handful of exact duplicate rows, like the real dataset occasionally has
orders += [orders[3].copy(), orders[10].copy()]

order_items = []
for i, o in enumerate(orders):
    n_items = random.randint(1, 3)
    for j in range(n_items):
        order_items.append({
            "order_id": o["order_id"],
            "order_item_id": j + 1,
            "product_id": f"prod_{random.randint(0, 80)}",
            "seller_id": f"seller_{random.randint(0, 20)}",
            "shipping_limit_date": o["order_purchase_timestamp"],
            "price": round(random.uniform(10, 500), 2),
            "freight_value": round(random.uniform(5, 50), 2),
        })

payments = []
for o in orders:
    n_installments = random.choice([1, 1, 1, 2, 3, 6])
    payments.append({
        "order_id": o["order_id"],
        "payment_sequential": 1,
        "payment_type": random.choice(["credit_card", "boleto", "voucher", "debit_card"]),
        "payment_installments": n_installments,
        "payment_value": round(random.uniform(20, 1000), 2),
    })

products = []
for i in range(81):
    products.append({
        "product_id": f"prod_{i}",
        "product_category_name": random.choice(categories),
    })

def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

write_csv(f"{OUT}/olist_customers_dataset.csv", customers, list(customers[0].keys()))
write_csv(f"{OUT}/olist_orders_dataset.csv", orders, list(orders[0].keys()))
write_csv(f"{OUT}/olist_order_items_dataset.csv", order_items, list(order_items[0].keys()))
write_csv(f"{OUT}/olist_order_payments_dataset.csv", payments, list(payments[0].keys()))
write_csv(f"{OUT}/olist_products_dataset.csv", products, list(products[0].keys()))

print(f"Wrote {len(customers)} customers, {len(orders)} orders (incl. 2 injected dupes), "
      f"{len(order_items)} order_items, {len(payments)} payments, {len(products)} products to {OUT}/")
