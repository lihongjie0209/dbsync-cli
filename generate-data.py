#!/usr/bin/env python3
"""
Continuous test-data generator for dbsync-cli manual testing.
Connects to the local MySQL container and generates INSERT / UPDATE / DELETE
events in a loop so you can watch the sync TUI react in real time.

Usage:
    python generate-data.py            # default: 1 event/sec, infinite
    python generate-data.py --rate 5   # 5 events/sec
    python generate-data.py --count 50 # stop after 50 events
"""

import argparse
import random
import string
import time
import sys

try:
    import mysql.connector
except ImportError:
    print("mysql-connector-python not found. Installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mysql-connector-python", "-q"])
    import mysql.connector

# ── connection ─────────────────────────────────────────────────────────────
DB = dict(host="localhost", port=13307, user="root", password="root", database="sourcedb")

NAMES   = ["Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Hiro","Iris","Jack",
           "Kara","Leo","Mia","Nora","Oscar","Pam","Quinn","Roy","Sara","Tom"]
EMAILS  = ["@gmail.com","@yahoo.com","@outlook.com","@example.com"]
PRODUCTS = ["Widget","Gadget","Doohickey","Thingamajig","Gizmo","Contraption",
            "Doodad","Whatchamacallit","Trinket","Knickknack"]
STATUSES = ["pending","paid","shipped","cancelled"]

def rand_str(n=6):
    return "".join(random.choices(string.ascii_lowercase, k=n))

def rand_email(name):
    return f"{name.lower()}.{rand_str(4)}{random.choice(EMAILS)}"

# ── operations ──────────────────────────────────────────────────────────────

def insert_user(cur):
    name  = random.choice(NAMES) + rand_str(3)
    email = rand_email(name)
    age   = random.randint(18, 70)
    cur.execute("INSERT INTO users (username, email, age) VALUES (%s, %s, %s)", (name, email, age))
    return f"INSERT  users  username={name}"

def update_user(cur):
    cur.execute("SELECT id FROM users ORDER BY RAND() LIMIT 1")
    row = cur.fetchone()
    if not row: return None
    uid   = row[0]
    email = rand_email(rand_str(5))
    cur.execute("UPDATE users SET email=%s WHERE id=%s", (email, uid))
    return f"UPDATE  users  id={uid} email={email}"

def delete_user(cur):
    # Only delete if there are > 3 rows to keep data alive
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] <= 3: return None
    cur.execute("SELECT id FROM users ORDER BY RAND() LIMIT 1")
    row = cur.fetchone()
    if not row: return None
    cur.execute("DELETE FROM users WHERE id=%s", (row[0],))
    return f"DELETE  users  id={row[0]}"

def insert_product(cur):
    name  = random.choice(PRODUCTS) + "-" + rand_str(4).upper()
    price = round(random.uniform(1.99, 999.99), 2)
    stock = random.randint(0, 500)
    cur.execute(
        "INSERT INTO products (name, price, stock, is_active) VALUES (%s, %s, %s, 1)",
        (name, price, stock))
    return f"INSERT  products  name={name}  price={price}"

def update_product(cur):
    cur.execute("SELECT id FROM products ORDER BY RAND() LIMIT 1")
    row = cur.fetchone()
    if not row: return None
    pid   = row[0]
    stock = random.randint(0, 500)
    price = round(random.uniform(1.99, 999.99), 2)
    cur.execute("UPDATE products SET stock=%s, price=%s WHERE id=%s", (stock, price, pid))
    return f"UPDATE  products  id={pid}  stock={stock}"

def insert_order(cur):
    cur.execute("SELECT id FROM users ORDER BY RAND() LIMIT 1")
    u = cur.fetchone()
    cur.execute("SELECT id, price FROM products ORDER BY RAND() LIMIT 1")
    p = cur.fetchone()
    if not u or not p: return None
    qty   = random.randint(1, 10)
    total = round(float(p[1]) * qty, 2)
    status = random.choice(STATUSES)
    cur.execute(
        "INSERT INTO orders (user_id, product_id, quantity, total, status) VALUES (%s,%s,%s,%s,%s)",
        (u[0], p[0], qty, total, status))
    return f"INSERT  orders  user={u[0]}  product={p[0]}  qty={qty}  total={total}"

def update_order(cur):
    cur.execute("SELECT id FROM orders ORDER BY RAND() LIMIT 1")
    row = cur.fetchone()
    if not row: return None
    oid    = row[0]
    status = random.choice(STATUSES)
    cur.execute("UPDATE orders SET status=%s WHERE id=%s", (status, oid))
    return f"UPDATE  orders  id={oid}  status={status}"

def delete_order(cur):
    cur.execute("SELECT COUNT(*) FROM orders")
    if cur.fetchone()[0] <= 3: return None
    cur.execute("SELECT id FROM orders ORDER BY RAND() LIMIT 1")
    row = cur.fetchone()
    if not row: return None
    cur.execute("DELETE FROM orders WHERE id=%s", (row[0],))
    return f"DELETE  orders  id={row[0]}"

# weighted pool: more inserts/updates, fewer deletes
OPS = (
    [insert_user]    * 3 +
    [update_user]    * 3 +
    [delete_user]    * 1 +
    [insert_product] * 2 +
    [update_product] * 3 +
    [insert_order]   * 4 +
    [update_order]   * 4 +
    [delete_order]   * 1
)

# ── main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate",  type=float, default=1.0,  help="events per second (default 1)")
    ap.add_argument("--count", type=int,   default=0,    help="stop after N events (0 = infinite)")
    args = ap.parse_args()

    delay = 1.0 / args.rate
    total = 0

    print(f"Connecting to MySQL at localhost:13307 …")
    conn = mysql.connector.connect(**DB)
    cur  = conn.cursor()
    print(f"Connected. Generating events at {args.rate}/sec  (Ctrl-C to stop)\n")
    print(f"{'#':>5}  {'time':8}  operation")
    print("-" * 60)

    try:
        while args.count == 0 or total < args.count:
            op  = random.choice(OPS)
            msg = op(cur)
            conn.commit()
            if msg:
                total += 1
                ts = time.strftime("%H:%M:%S")
                print(f"{total:>5}  {ts}  {msg}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print(f"\nStopped after {total} events.")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
