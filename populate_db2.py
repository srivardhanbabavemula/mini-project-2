# populate_db.py  (updated - streaming + batched order inserts)
import psycopg2
from psycopg2 import extras
from datetime import datetime
from utils import get_db_url
import time
import sys
import csv

DATA_FILE = "data.csv"

DDL_SQL = """
DROP TABLE IF EXISTS orderdetail CASCADE;
DROP TABLE IF EXISTS product CASCADE;
DROP TABLE IF EXISTS productcategory CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS country CASCADE;
DROP TABLE IF EXISTS region CASCADE;

CREATE TABLE region (
    regionid   SERIAL PRIMARY KEY,
    region     TEXT NOT NULL
);

CREATE TABLE country (
    countryid  SERIAL PRIMARY KEY,
    country    TEXT NOT NULL,
    regionid   INTEGER NOT NULL REFERENCES region(regionid)
);

CREATE TABLE customer (
    customerid SERIAL PRIMARY KEY,
    firstname  TEXT NOT NULL,
    lastname   TEXT NOT NULL,
    address    TEXT NOT NULL,
    city       TEXT NOT NULL,
    countryid  INTEGER NOT NULL REFERENCES country(countryid)
);

CREATE TABLE productcategory (
    productcategoryid SERIAL PRIMARY KEY,
    productcategory   TEXT NOT NULL,
    productcategorydescription TEXT NOT NULL
);

CREATE TABLE product (
    productid   SERIAL PRIMARY KEY,
    productname TEXT NOT NULL,
    productunitprice REAL NOT NULL,
    productcategoryid INTEGER NOT NULL REFERENCES productcategory(productcategoryid)
);

CREATE TABLE orderdetail (
    orderid        SERIAL PRIMARY KEY,
    customerid     INTEGER NOT NULL REFERENCES customer(customerid),
    productid      INTEGER NOT NULL REFERENCES product(productid),
    orderdate      DATE NOT NULL,
    quantityordered INTEGER NOT NULL
);
"""

# Increase CSV field size limit for very large fields
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(10**9)


def parse_regions(path):
    regions = set()
    with open(path, encoding="utf-8") as f:
        # try detect header (we assume first line is header)
        next(f)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) > 4 and parts[4].strip():
                regions.add(parts[4].strip())
    return sorted(regions)


def parse_countries(path):
    pairs = set()
    with open(path, encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) > 4:
                country = parts[3].strip()
                region = parts[4].strip()
                if country and region:
                    pairs.add((country, region))
    return sorted(pairs, key=lambda x: x[0])


def parse_productcategories(path):
    cats = set()
    with open(path, encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) > 7:
                # guard against missing fields
                pc_raw = parts[6] or ""
                pcd_raw = parts[7] or ""
                cat_list = [c.strip() for c in pc_raw.split(";") if c.strip()]
                desc_list = [d.strip() for d in pcd_raw.split(";")] if pcd_raw else []
                for i, cat in enumerate(cat_list):
                    desc = desc_list[i] if i < len(desc_list) else ""
                    cats.add((cat, desc))
    return sorted(cats, key=lambda x: x[0])


def parse_products(path):
    prods = set()
    with open(path, encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            # guard length
            if len(parts) > 8:
                names_raw = parts[5] or ""
                cats_raw = parts[6] or ""
                prices_raw = parts[8] or ""
                names = [n.strip() for n in names_raw.split(";") if n.strip()]
                cats = [c.strip() for c in cats_raw.split(";")] if cats_raw else []
                prices = [p.strip() for p in prices_raw.split(";")] if prices_raw else []
                for i, n in enumerate(names):
                    if not n:
                        continue
                    cat = cats[i] if i < len(cats) else ""
                    price_raw = prices[i] if i < len(prices) else ""
                    try:
                        price = float(price_raw) if price_raw else 0.0
                    except Exception:
                        try:
                            price = float(price_raw.replace(",", "")) if price_raw else 0.0
                        except Exception:
                            price = 0.0
                    if cat:
                        prods.add((n, cat, price))
    return sorted(prods, key=lambda x: x[0])


def parse_customers(path, valid_countries):
    custs = set()
    with open(path, encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) > 4:
                name = (parts[0] or "").strip()
                address = (parts[1] or "").strip()
                city = (parts[2] or "").strip()
                country = (parts[3] or "").strip()
                if not country or country not in valid_countries:
                    continue
                if not name:
                    continue
                name_parts = name.split()
                first = name_parts[0]
                last = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                custs.add((first, last, address, city, country))
    return sorted(custs, key=lambda x: (x[0] + " " + x[1]))


def parse_orders_stream(path):
    """
    Generator that yields tuples (name, customer_address, city, country, prod_names_list, qtys_list, dates_list)
    Splits the product lists and yields one line at a time.
    """
    with open(path, encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            parts = line.rstrip("\n").split("\t")
            # be tolerant of short lines
            if len(parts) < 6:
                continue
            name = " ".join(parts[0].split()).strip()
            address = (parts[1] or "").strip() if len(parts) > 1 else ""
            city = (parts[2] or "").strip() if len(parts) > 2 else ""
            country = (parts[3] or "").strip() if len(parts) > 3 else ""
            productnames = (parts[5] or "").strip() if len(parts) > 5 else ""
            productunitprice = (parts[8] or "").strip() if len(parts) > 8 else ""
            productcategory = (parts[6] or "").strip() if len(parts) > 6 else ""
            qtys = (parts[9] or "").strip() if len(parts) > 9 else ""
            dates = (parts[10] or "").strip() if len(parts) > 10 else ""

            yield (name, address, city, country, productnames, productunitprice, productcategory, qtys, dates)


def main(batch_size_orders=5000):
    db_url = get_db_url()
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    print("Dropping and creating tables...")
    cur.execute(DDL_SQL)
    conn.commit()
    print("✅ Tables created")

    # ---------- REGION ----------
    print("Inserting regions...")
    regions = parse_regions(DATA_FILE)
    if regions:
        extras.execute_batch(cur, "INSERT INTO region (region) VALUES (%s)", [(r,) for r in regions], page_size=1000)
        conn.commit()
    cur.execute("SELECT region, regionid FROM region")
    region_map = {r: rid for r, rid in cur.fetchall()}

    # ---------- COUNTRY ----------
    print("Inserting countries...")
    country_pairs = parse_countries(DATA_FILE)
    country_rows = [(country, region_map[region]) for (country, region) in country_pairs if region in region_map]
    if country_rows:
        extras.execute_batch(cur, "INSERT INTO country (country, regionid) VALUES (%s, %s)", country_rows, page_size=1000)
        conn.commit()
    cur.execute("SELECT country, countryid FROM country")
    country_map = {c: cid for c, cid in cur.fetchall()}

    # ---------- PRODUCT CATEGORY ----------
    print("Inserting product categories...")
    categories = parse_productcategories(DATA_FILE)
    if categories:
        extras.execute_batch(cur, "INSERT INTO productcategory (productcategory, productcategorydescription) VALUES (%s, %s)", categories, page_size=1000)
        conn.commit()
    cur.execute("SELECT productcategory, productcategoryid FROM productcategory")
    cat_map = {c: cid for c, cid in cur.fetchall()}

    # ---------- PRODUCT ----------
    print("Inserting products...")
    products_raw = parse_products(DATA_FILE)
    product_rows = [(name, price, cat_map[cat]) for (name, cat, price) in products_raw if cat in cat_map]
    if product_rows:
        extras.execute_batch(cur, "INSERT INTO product (productname, productunitprice, productcategoryid) VALUES (%s, %s, %s)", product_rows, page_size=1000)
        conn.commit()
    cur.execute("SELECT productname, productid FROM product")
    product_map = {n: pid for n, pid in cur.fetchall()}

    # ---------- CUSTOMER ----------
    print("Inserting customers...")
    customers_raw = parse_customers(DATA_FILE, set(country_map.keys()))
    customer_rows = [(first, last, address, city, country_map[country]) for (first, last, address, city, country) in customers_raw]
    if customer_rows:
        extras.execute_batch(cur, "INSERT INTO customer (firstname, lastname, address, city, countryid) VALUES (%s, %s, %s, %s, %s)", customer_rows, page_size=1000)
        conn.commit()
    cur.execute("SELECT firstname, lastname, customerid FROM customer")
    cust_map = {f"{f} {l}".strip(): cid for f, l, cid in cur.fetchall()}

    # ---------- ORDERDETAIL (stream + batch insert) ----------
    print("Inserting order details (streaming + batched inserts)...")
    start_time = time.time()
    pg_cur = conn.cursor()

    insert_rows = []
    total_inserted = 0
    processed_lines = 0

    # We'll need a quick lookup of country name -> countryid for resolving customer keys if needed
    # country_map already has that

    # iterate source rows
    parse_orders_iterable = parse_orders_stream(DATA_FILE)
    for (name, address, city, country, pnames_raw, prices_raw, pcats_raw, qtys_raw, dates_raw) in parse_orders_iterable :
        processed_lines += 1
        # resolve customer id from cust_map by First Last
        if not name:
            continue
        name_parts = name.split()
        first = name_parts[0]
        last = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
        customer_key = f"{first} {last}".strip()
        customer_id = cust_map.get(customer_key)
        if customer_id is None:
            # fallback: try to find customer by first+last+country
            if country:
                cid = country_map.get(country)
                if cid:
                    pg_cur.execute("SELECT customerid FROM customer WHERE firstname=%s AND lastname=%s AND countryid=%s LIMIT 1", (first, last, cid))
                    rr = pg_cur.fetchone()
                    if rr:
                        customer_id = rr[0]
            if customer_id is None:
                # if we can't resolve, skip this customer's order lines
                continue

        # prepare lists
        pnames = [p.strip() for p in (pnames_raw or "").split(";")] if pnames_raw else []
        qtys = [q.strip() for q in (qtys_raw or "").split(";")] if qtys_raw else []
        dates = [d.strip() for d in (dates_raw or "").split(";")] if dates_raw else []
        prices = [p.strip() for p in (prices_raw or "").split(";")] if prices_raw else []
        pcats = [pc.strip() for pc in (pcats_raw or "").split(";")] if pcats_raw else []

        max_len = max(len(pnames), len(qtys), len(dates), len(prices), len(pcats))
        for i in range(max_len):
            pname = pnames[i] if i < len(pnames) else ""
            if not pname:
                continue

            # quantity
            qty = 0
            if i < len(qtys):
                try:
                    qty = int(float(qtys[i]))
                except Exception:
                    try:
                        qty = int(float(qtys[i].replace(",", "")))
                    except Exception:
                        qty = 0

            # date normalization
            date_str = None
            if i < len(dates):
                d_raw = dates[i].strip()
                if len(d_raw) == 8 and d_raw.isdigit():
                    try:
                        date_str = datetime.strptime(d_raw, "%Y%m%d").date()
                    except Exception:
                        date_str = None
                else:
                    # try parse common formats, else skip storing date
                    try:
                        date_str = datetime.fromisoformat(d_raw).date()
                    except Exception:
                        try:
                            # day/month/year or etc - best-effort
                            date_str = datetime.strptime(d_raw, "%Y-%m-%d").date()
                        except Exception:
                            date_str = None

            if date_str is None:
                # if no valid date, skip this order line
                continue

            product_id = product_map.get(pname)
            if product_id is None:
                # Optionally: try to insert missing product on-the-fly using price & category (skipped here)
                continue

            insert_rows.append((customer_id, product_id, date_str, qty))

            # When batch is full, flush to DB
            if len(insert_rows) >= batch_size_orders:
                try:
                    extras.execute_values(pg_cur,
                                          "INSERT INTO orderdetail (customerid, productid, orderdate, quantityordered) VALUES %s",
                                          insert_rows,
                                          page_size=1000)
                    conn.commit()
                    total_inserted += len(insert_rows)
                    elapsed = time.time() - start_time
                    print(f"Inserted {total_inserted:,} order rows (processed {processed_lines:,} input lines) — elapsed {elapsed:.1f}s")
                    insert_rows = []
                except Exception as e:
                    conn.rollback()
                    print("Error inserting batch of orderdetail rows:", e)
                    # try a per-row insert fallback for the batch (skip harmful rows)
                    for r in insert_rows:
                        try:
                            pg_cur.execute("INSERT INTO orderdetail (customerid, productid, orderdate, quantityordered) VALUES (%s, %s, %s, %s)", r)
                            conn.commit()
                            total_inserted += 1
                        except Exception as e2:
                            conn.rollback()
                            print("Skipping problematic order row due to error:", e2)
                    insert_rows = []

    # final flush
    if insert_rows:
        try:
            extras.execute_values(pg_cur,
                                  "INSERT INTO orderdetail (customerid, productid, orderdate, quantityordered) VALUES %s",
                                  insert_rows,
                                  page_size=1000)
            conn.commit()
            total_inserted += len(insert_rows)
            elapsed = time.time() - start_time
            print(f"Inserted final {len(insert_rows):,} order rows — total {total_inserted:,} — elapsed {elapsed:.1f}s")
        except Exception as e:
            conn.rollback()
            print("Error inserting final batch:", e)

    pg_cur.close()
    cur.close()
    conn.close()
    print("✅ Finished populating mini-project2 sales database")
    print(f"Total orderdetail rows inserted: {total_inserted:,}")


if __name__ == "__main__":
    # you can pass smaller batch sizes for testing
    main(batch_size_orders=5000)