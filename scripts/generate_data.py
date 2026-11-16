#!/usr/bin/env python3
"""Synthetic source-system data for Larkspur & Pine Brewing Co. (fictional).

Simulates four source systems and writes files into a landing zone, the way a
real ingestion setup would receive them:

    data/landing/erp/<yyyy-mm-dd>/*.csv        ERP: customers, items, prices, orders, shipments, invoices
    data/landing/prod/<yyyy-mm-dd>/*.json      Brewery production: batches, ingredients, packaging, costs (JSON Lines)
    data/landing/pos/<yyyy-mm-dd>/*.json       Taproom POS: transactions with nested line items (JSON Lines)
    data/landing/dist/<yyyy-mm-dd>/*           Distributor depletion reports, three different layouts

Usage:
    python scripts/generate_data.py backfill [--business-date 2026-10-31] [--clean] [--write-seeds] [--force]
    python scripts/generate_data.py advance  [--days 1]

`backfill` writes ~18 months of history as a full extract dated the business date.
`advance` writes the next business day's incremental files (and any scheduled
surprises: a customer move, a price change, a re-sent POS file, a late distributor file, and on the
first day the corrected packaging run that clears the act 1 yield error).

Everything is deterministic for a given --seed: every day's activity comes from a
random stream keyed on (seed, system, date), so `advance` reproduces exactly what
a longer backfill would have contained.

Standard library only. All company, customer and product names are invented.
Tax rates and prices are illustrative, not real.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

DEFAULT_SEED = 20251
DEFAULT_START = "2025-05-01"
DEFAULT_BUSINESS_DATE = "2026-10-31"
GAL_PER_BBL = 31.0

# package_code -> short code, description, ERP unit of measure, gallons per unit,
# list price by tier (A distributor, B key account, C standard, D event/list)
PACKAGES = {
    "KEG-HALF": ("K50", "1/2 bbl keg", "KEG", 15.5, (135.0, 165.0, 175.0, 195.0)),
    "KEG-SIXTH": ("K17", "1/6 bbl keg", "KEG", 5.1667, (62.0, 76.0, 82.0, 92.0)),
    "CS-24x16": ("C16", "Case 24 x 16 oz cans", "CASE", 3.0, (30.0, 36.0, 38.0, 44.0)),
    "CS-24x12": ("C12", "Case 24 x 12 oz cans", "CASE", 2.25, (24.0, 29.0, 31.0, 36.0)),
}
TIERS = ("A", "B", "C", "D")

# code, name, style, category, abv, popularity, price multiplier, season months (None = year-round), lager?
BRANDS = [
    ("NSL", "North Shore Lager", "American Lager", "BEER", 4.8, 3.0, 1.00, None, True),
    ("PBI", "Pine Barrens IPA", "IPA", "BEER", 6.7, 3.0, 1.10, None, False),
    ("LKB", "Larkspur Blonde", "Blonde Ale", "BEER", 4.6, 2.5, 1.00, None, False),
    ("HLI", "Hazy Lakeshore IPA", "Hazy IPA", "BEER", 6.4, 2.5, 1.12, None, False),
    ("TPA", "Tamarack Pale Ale", "Pale Ale", "BEER", 5.4, 1.5, 1.04, None, False),
    ("PPL", "Portage Pilsner", "Pilsner", "BEER", 5.0, 1.5, 1.02, None, True),
    ("IRS", "Iron Ridge Stout", "Stout", "BEER", 6.0, 1.0, 1.06, None, False),
    ("PSP", "Prairie Smoke Porter", "Porter", "BEER", 5.8, 1.0, 1.05, None, False),
    ("BBA", "Birch Bark Amber", "Amber Ale", "BEER", 5.2, 1.2, 1.02, None, False),
    ("LCK", "Loon Call Kolsch", "Kolsch", "BEER", 4.9, 1.2, 1.02, None, False),
    ("WRW", "Wild Rice Wheat", "Wheat Ale", "BEER", 5.1, 1.0, 1.03, None, False),
    ("DWD", "Deep Winter Double IPA", "Double IPA", "BEER", 8.2, 0.8, 1.28, None, False),
    ("SDS", "Summer Dock Shandy", "Shandy", "BEER", 4.2, 1.6, 1.00, (4, 5, 6, 7, 8), False),
    ("HFM", "Harvest Fest Marzen", "Marzen", "BEER", 5.7, 1.4, 1.05, (8, 9, 10), True),
    ("WWA", "Woodstove Winter Ale", "Winter Ale", "BEER", 7.0, 1.2, 1.12, (11, 12, 1, 2), False),
    ("TSB", "Thaw Spring Bock", "Bock", "BEER", 6.5, 1.0, 1.08, (2, 3, 4), True),
    ("SZL", "Pine & Lime Hard Seltzer", "Hard Seltzer", "SELTZER", 5.0, 1.4, 0.98, None, False),
    ("SZB", "Bog Berry Hard Seltzer", "Hard Seltzer", "SELTZER", 5.0, 1.2, 0.98, None, False),
    ("SZP", "Peach Orchard Hard Seltzer", "Hard Seltzer", "SELTZER", 5.0, 1.0, 0.98, None, False),
    ("SZC", "Cucumber Mint Hard Seltzer", "Hard Seltzer", "SELTZER", 5.0, 0.8, 0.98, None, False),
    ("NAG", "Easy Paddle NA Golden", "Non-Alcoholic", "NA", 0.4, 0.9, 0.92, None, False),
    ("NAI", "Easy Paddle NA IPA", "Non-Alcoholic", "NA", 0.4, 0.8, 0.94, None, False),
    ("NAH", "Still Water Hop Water", "Hop Water", "NA", 0.0, 0.6, 0.80, None, False),
    # launched late: its distributor SKU codes are the "unmapped SKU" defect
    ("CBS", "Cranberry Bog Sour", "Sour Ale", "BEER", 5.3, 1.3, 1.15, None, False),
]
FLAGSHIPS = {"NSL", "PBI", "LKB", "HLI"}
NEW_BRAND = "CBS"
NEW_BRAND_LAUNCH_OFFSET = -60  # days relative to the backfill business date

SEASON = [0.80, 0.80, 0.90, 1.00, 1.15, 1.30, 1.35, 1.30, 1.10, 1.00, 0.90, 1.00]

DISTRIBUTORS = [
    # code, erp customer_no, name, city, state, size factor, order weekdays, report lag days
    ("NORTHLAND", "D-1001", "Northland Beverage Partners", "Minneapolis", "MN", 1.00, (0, 3), 8),
    ("BADGER", "D-1002", "Badger Ridge Distributing", "Eau Claire", "WI", 0.60, (1, 4), 12),
    ("HAWKEYE", "D-1003", "Hawkeye Prairie Wholesale", "Des Moines", "IA", 0.40, (0, 2), 15),
]
CITIES = {
    "MN": ["Minneapolis", "St. Paul", "Bloomington", "Edina", "Roseville", "Maple Grove", "Eagan",
           "Stillwater", "Minnetonka", "Woodbury", "Duluth", "Rochester", "St. Cloud", "Mankato"],
    "WI": ["Eau Claire", "La Crosse", "Madison", "Hudson", "Menomonie", "Wausau", "Superior", "Green Bay"],
    "IA": ["Des Moines", "Ames", "Cedar Rapids", "Iowa City", "Mason City", "Decorah", "Dubuque"],
}
METRO = CITIES["MN"][:10]
STATE_VARIANTS = {"MN": ["MN", "MN", "MN", "MN", "Minnesota", "Minn.", "mn", " MN "]}
NAME_A = ["Copper", "Lakeside", "North Star", "Blue Heron", "Red Pine", "Old Mill", "Iron", "Golden",
          "River", "Prairie", "Cedar", "Stone", "Harbor", "Maple", "Foxtail", "Driftwood", "Silver",
          "Timber", "Juniper", "Granite", "Sunset", "Whistle", "Anchor", "Birch", "Meadow", "Summit"]
NAME_B = ["Kettle", "Lantern", "Canoe", "Oak", "Anvil", "Bridge", "Hollow", "Crossing", "Point",
          "Landing", "Corner", "Depot", "Station", "Barrel", "Compass", "Orchard", "Quarry", "Wharf"]
TYPE_ON = ["Tavern", "Bar & Grill", "Taphouse", "Pub", "Supper Club", "Pizza Co.", "Kitchen", "Bowl"]
TYPE_OFF = ["Liquor", "Wine & Spirits", "Bottle Shop", "Market", "Beverage Depot", "Liquor Barn"]
STREETS = ["Main St", "Lake Ave", "1st Ave N", "Hennepin Ave", "Grand Ave", "County Rd 5", "Oak St",
           "Washington Ave", "Mill Rd", "Depot St", "University Ave", "Cedar Ln", "Broadway"]
PEOPLE = ["A. Lindqvist", "M. Okafor", "J. Halvorsen", "R. Castillo", "T. Nguyen", "S. Bergstrom"]
TANKS = ["FV-01", "FV-02", "FV-03", "FV-04", "FV-05", "FV-06", "FV-07", "FV-08"]


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def rng_for(seed: int, *keys) -> random.Random:
    return random.Random("|".join([str(seed), *map(str, keys)]))


def daterange(a: date, b: date):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def add_business_days(d: date, n: int) -> date:
    while n > 0:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d


def ts(d: date, r: random.Random, lo=7, hi=18) -> str:
    return datetime(d.year, d.month, d.day, r.randint(lo, hi), r.randint(0, 59), r.randint(0, 59)).isoformat(sep=" ")


def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def month_bounds(ym: str) -> tuple[date, date]:
    y, m = map(int, ym.split("-"))
    first = date(y, m, 1)
    nxt = date(y + (m == 12), (m % 12) + 1, 1)
    return first, nxt - timedelta(days=1)


def write_csv(path: Path, fields: list[str], rows: list[dict], delimiter=",") -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter=delimiter, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return len(rows)


def write_jsonl(path: Path, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps({k: v for k, v in row.items() if not k.startswith("_")}) + "\n")
    return len(rows)


# ----------------------------------------------------------------------------
# the simulated world
# ----------------------------------------------------------------------------
class World:
    def __init__(self, seed: int, start: date, backfill_date: date, clean: bool):
        self.seed, self.start, self.bf, self.clean = seed, start, backfill_date, clean
        self.launch = backfill_date + timedelta(days=NEW_BRAND_LAUNCH_OFFSET)
        self.brand = {b[0]: b for b in BRANDS}
        self._build_items()
        self._build_prices()
        self._build_customers()
        self._build_dist_accounts()
        self._erp_cache: dict | None = None
        self._erp_through: date | None = None

    # ---- master data --------------------------------------------------------
    def _build_items(self):
        r = rng_for(self.seed, "items")
        self.items, self.items_by_brand = {}, defaultdict(list)
        for code, name, style, cat, abv, pop, mult, months, lager in BRANDS:
            if cat == "BEER":
                pkgs = ["KEG-HALF", "KEG-SIXTH", "CS-24x16"] + (["CS-24x12"] if code in FLAGSHIPS else [])
            elif cat == "SELTZER":
                pkgs = ["KEG-SIXTH", "CS-24x12"]
            else:
                pkgs = ["CS-24x12"]
            created = self.launch - timedelta(days=30) if code == NEW_BRAND else self.start - timedelta(days=400)
            for p in pkgs:
                short, desc, uom, gal, prices = PACKAGES[p]
                item_no = f"{code}-{short}"
                self.items[item_no] = {
                    "item_no": item_no, "description": f"{name} {desc}", "brand_code": code,
                    "brand_name": name, "style": style, "category": cat, "abv": abv,
                    "package_code": p, "base_uom": uom,
                    "standard_cost": round(prices[0] * mult * r.uniform(0.50, 0.60), 2),
                    "status": "ACTIVE", "created_date": created.isoformat(),
                    "updated_at": f"{created.isoformat()} 09:00:00", "_created": created,
                }
                self.items_by_brand[code].append(item_no)

    def brand_active(self, code: str, d: date, lead_days: int = 0) -> bool:
        b = self.brand[code]
        if code == NEW_BRAND and d + timedelta(days=lead_days) < self.launch:
            return False
        months = b[7]
        if months is None:
            return True
        return (d + timedelta(days=lead_days)).month in months

    def active_items(self, d: date) -> list[str]:
        return [i for i, it in self.items.items() if self.brand_active(it["brand_code"], d)]

    def _build_prices(self):
        """ERP price list is effective-dated (starting_date / ending_date), Business Central style."""
        self.price_versions = defaultdict(list)  # (item, tier) -> [(starting_date, price)]
        for item_no, it in self.items.items():
            mult = self.brand[it["brand_code"]][6]
            for tier, base in zip(TIERS, PACKAGES[it["package_code"]][4]):
                self.price_versions[(item_no, tier)].append((it["_created"], round(base * mult, 2)))
        changes = []  # (date, predicate, factor)
        for y in range(self.start.year + 1, self.bf.year + 2):
            changes.append((date(y, 1, 1), lambda it: True, 1.04))
        changes.append((self.bf + timedelta(days=2), lambda it: it["brand_code"] == "PBI", 1.06))
        for d, pred, factor in sorted(changes, key=lambda c: c[0]):
            for item_no, it in self.items.items():
                if pred(it) and it["_created"] <= d:
                    for tier in TIERS:
                        v = self.price_versions[(item_no, tier)]
                        v.append((d, round(v[-1][1] * factor, 2)))

    def price_at(self, item_no: str, tier: str, d: date) -> float:
        price = None
        for start, p in self.price_versions[(item_no, tier)]:
            if start <= d:
                price = p
        return price if price is not None else self.price_versions[(item_no, tier)][0][1]

    def price_rows(self, through: date, only_day: date | None = None) -> list[dict]:
        rows = []
        for (item_no, tier), versions in self.price_versions.items():
            vs = [v for v in versions if v[0] <= through]
            for i, (start, price) in enumerate(vs):
                end = vs[i + 1][0] - timedelta(days=1) if i + 1 < len(vs) else None
                changed_on = vs[i + 1][0] if end else start
                if only_day and changed_on != only_day and start != only_day:
                    continue
                rows.append({"item_no": item_no, "price_tier": tier, "unit_price": f"{price:.2f}",
                             "currency": "USD", "starting_date": start.isoformat(),
                             "ending_date": end.isoformat() if end else "",
                             "updated_at": f"{max(changed_on, start).isoformat()} 06:00:00"})
        return rows

    def _unique_name(self, r, used, types):
        while True:
            n = f"{r.choice(NAME_A)} {r.choice(NAME_B)} {r.choice(types)}"
            if n not in used:
                used.add(n)
                return n

    def _build_customers(self):
        r = rng_for(self.seed, "customers")
        used: set[str] = set()
        created = self.start - timedelta(days=300)
        base = []

        def add(no, name, ctype, city, state, tier, terr):
            base.append({"customer_no": no, "customer_name": name, "customer_type": ctype,
                         "address_line1": f"{r.randint(100, 9899)} {r.choice(STREETS)}", "city": city,
                         "state": state, "postal_code": f"{r.randint(55001, 56799)}", "price_tier": tier,
                         "territory": terr, "salesperson": r.choice(PEOPLE), "payment_terms": "NET30",
                         "created_date": created.isoformat(), "updated_at": f"{created.isoformat()} 10:00:00",
                         "_d": created})

        for code, no, name, city, state, *_ in DISTRIBUTORS:
            add(no, name, "DISTRIBUTOR", city, state, "A", "WHOLESALE")
        for i in range(40):
            on = r.random() < 0.55
            city = r.choice(METRO)
            add(f"C-{2001 + i}", self._unique_name(r, used, TYPE_ON if on else TYPE_OFF),
                "ON_PREMISE" if on else "RETAIL", city, r.choice(STATE_VARIANTS["MN"]),
                "B" if r.random() < 0.15 else "C", "METRO-EAST" if METRO.index(city) % 2 else "METRO-WEST")
        events = ["Twin Ports Brew Fest", "Lakes Area Chili Cook-Off", "Riverfront Music Series",
                  "Harvest Moon 10K", "Northwoods Wedding Co.", "Summit Corporate Events",
                  "Red River Valley Brew Fest", "Midsummer Makers Market"]
        for i, name in enumerate(events):
            if name.startswith("Red River"):
                add(f"E-{3001 + i}", name, "EVENT", "Fargo", "ND", "D", "EVENTS")  # ND has no excise rate seeded
            else:
                add(f"E-{3001 + i}", name, "EVENT", r.choice(METRO), "MN", "D", "EVENTS")
        if not self.clean:
            base[3 + 11]["state"] = "Minnesotta"  # typo that the state seed does not map
        self.customer_versions = defaultdict(list)
        for c in base:
            self.customer_versions[c["customer_no"]].append(c)

        def change(no, d, **updates):
            new = dict(self.customer_versions[no][-1])
            new.update(updates)
            new["updated_at"] = f"{d.isoformat()} 08:15:00"
            new["_d"] = d
            self.customer_versions[no].append(new)

        # scheduled after the backfill so `advance` exercises snapshots (SCD2)
        # day 1: a tier-C bar moves across the metro and is upgraded to tier B (key account)
        mover = next(no for no, vs in sorted(self.customer_versions.items())
                     if no.startswith("C-") and no != "C-2021"
                     and vs[0]["customer_type"] == "ON_PREMISE" and vs[0]["price_tier"] == "C")
        self.tier_change_customer = mover
        change(mover, self.bf + timedelta(days=1), address_line1="4100 Depot St", city="Woodbury",
               territory="METRO-EAST", price_tier="B")
        change("C-2021", self.bf + timedelta(days=4), salesperson="S. Bergstrom", territory="METRO-WEST")

    def customer_at(self, no: str, d: date) -> dict:
        cur = self.customer_versions[no][0]
        for v in self.customer_versions[no]:
            if v["_d"] <= d:
                cur = v
        return cur

    def _build_dist_accounts(self):
        self.dist_accounts, self.dist_sku = {}, {}
        for code, _, _, _, state, *_ in DISTRIBUTORS:
            r = rng_for(self.seed, "dist_accounts", code)
            used: set[str] = set()
            accts = []
            for i in range(55):
                on = r.random() < 0.5
                acct_id = {"NORTHLAND": f"A{r.randint(100000, 999999):07d}",
                           "BADGER": f"WI-{10000 + i * 7 + r.randint(0, 6)}",
                           "HAWKEYE": f"{30000 + i * 11 + r.randint(0, 9)}"}[code]
                accts.append({"id": acct_id, "name": self._unique_name(r, used, TYPE_ON if on else TYPE_OFF),
                              "city": r.choice(CITIES[state]), "state": state, "on": on,
                              "weight": r.lognormvariate(0, 0.8)})
            self.dist_accounts[code] = accts
            sku = {}
            for n, (item_no, it) in enumerate(self.items.items()):
                badger_pkg = {"K50": "HB", "K17": "SB", "C16": "16C", "C12": "12C"}[PACKAGES[it["package_code"]][0]]
                sku[item_no] = {"NORTHLAND": f"NB{20000 + n * 13 + r.randint(0, 12)}",
                                "BADGER": f"LP{it['brand_code']}-{badger_pkg}",
                                "HAWKEYE": f"{600000 + n * 17 + r.randint(0, 16)}"}[code]
            self.dist_sku[code] = sku

    # ---- ERP transactions ---------------------------------------------------
    def _order_lines(self, r, cust, d, kind, factor=1.0):
        active = self.active_items(d)
        season = SEASON[d.month - 1]
        if kind == "DIST":
            k = min(len(active), r.randint(12, 22))
        elif kind == "EVENT":
            k = r.randint(3, 8)
        else:
            k = r.randint(2, 6)
        weights = [self.brand[self.items[i]["brand_code"]][5] for i in active]
        chosen = []
        pool = list(zip(active, weights))
        for _ in range(k):
            if not pool:
                break
            pick = r.choices(range(len(pool)), weights=[w for _, w in pool])[0]
            chosen.append(pool.pop(pick)[0])
        lines = []
        for item_no in chosen:
            it = self.items[item_no]
            keg = it["base_uom"] == "KEG"
            if cust["customer_type"] == "RETAIL" and keg:
                continue
            if cust["customer_type"] == "ON_PREMISE" and not keg and r.random() < 0.7:
                continue
            pop = self.brand[it["brand_code"]][5]
            if kind == "DIST":
                q = (r.randint(2, 7) if keg else r.randint(8, 48)) * factor * season * (0.6 + pop / 3)
            elif kind == "EVENT":
                q = r.randint(2, 10) if keg else r.randint(5, 30)
            else:
                q = r.randint(1, 4) if keg else r.randint(2, 15)
            disc = 0.0 if kind == "DIST" else r.choices([0.0, 5.0, 10.0], weights=[85, 10, 5])[0]
            lines.append((item_no, max(1, int(round(q))), self.price_at(item_no, cust["price_tier"], d), disc))
        return lines

    def erp(self, through: date) -> dict:
        """All ERP transaction rows whose event date is <= through. Each row carries `_d`."""
        if self._erp_cache is not None and self._erp_through == through:
            return self._erp_cache
        headers, lines, ships, invs = [], [], [], []
        for d in daterange(self.start, through):
            if d.weekday() >= 5:
                continue
            r = rng_for(self.seed, "erp", d)
            todays = []
            for code, no, _, _, _, factor, weekdays, _ in DISTRIBUTORS:
                if d.weekday() in weekdays:
                    todays.append((no, "DIST", factor))
            for no in sorted(self.customer_versions):
                if no.startswith("C-") and r.random() < 0.14:
                    todays.append((no, "DIRECT", 1.0))
                elif no.startswith("E-") and r.random() < 0.015 * SEASON[d.month - 1]:
                    todays.append((no, "EVENT", 1.0))
            for seq, (no, kind, factor) in enumerate(todays, start=1):
                cust = self.customer_at(no, d)
                olines = self._order_lines(r, cust, d, kind, factor)
                if not olines:
                    continue
                order_no = f"SO-{d:%y%m%d}-{seq:03d}"
                cancelled = r.random() < 0.01
                ship_date = add_business_days(d, r.randint(1, 3))
                head = {"order_no": order_no, "customer_no": no, "order_date": d.isoformat(),
                        "requested_ship_date": ship_date.isoformat(), "status": "OPEN",
                        "order_source": {"DIST": "EDI", "DIRECT": "SALES_REP", "EVENT": "PHONE"}[kind],
                        "updated_at": ts(d, r), "_d": d}
                headers.append(head)
                for ln, (item_no, qty, price, disc) in enumerate(olines, start=1):
                    lines.append({"order_no": order_no, "line_no": ln * 10000, "item_no": item_no,
                                  "quantity": qty, "unit_of_measure": self.items[item_no]["base_uom"],
                                  "unit_price": f"{price:.2f}", "line_discount_pct": disc, "_d": d})
                if cancelled:
                    nd = add_business_days(d, 1)
                    headers.append({**head, "status": "CANCELLED", "updated_at": ts(nd, r), "_d": nd})
                    continue
                headers.append({**head, "status": "INVOICED", "updated_at": ts(ship_date, r), "_d": ship_date})
                ship_no = f"SH-{order_no[3:]}"
                ships.append({"shipment_no": ship_no, "order_no": order_no, "customer_no": no,
                              "ship_date": ship_date.isoformat(), "ship_from": "BREWERY-WH",
                              "carrier": "CUSTOMER PICKUP" if kind == "EVENT" else r.choice(["LPB FLEET", "LPB FLEET", "NORTHSTAR FREIGHT"]),
                              "posted_at": ts(ship_date, r), "_d": ship_date})
                orphan = (not self.clean) and r.random() < 0.006
                inv_no = f"INV-{order_no[3:]}"
                for ln, (item_no, qty, price, disc) in enumerate(olines, start=1):
                    amount = round(qty * price * (1 - disc / 100), 2)
                    inv = {"invoice_no": inv_no, "invoice_line_no": ln * 10000, "document_type": "INVOICE",
                           "invoice_date": ship_date.isoformat(), "order_no": order_no, "order_line_no": ln * 10000,
                           "customer_no": no, "item_no": item_no, "quantity": qty, "unit_price": f"{price:.2f}",
                           "line_discount_pct": disc, "line_amount": f"{amount:.2f}",
                           "shipment_no": ship_no.replace("SH-", "SH-X") if orphan else ship_no,
                           "posted_at": ts(ship_date, r), "_d": ship_date, "_kind": kind}
                    invs.append(inv)
                    if kind != "DIST" and r.random() < 0.012:  # a return: credit memo line with negative quantity
                        rd = ship_date + timedelta(days=r.randint(5, 20))
                        rq = -max(1, qty // r.randint(2, 4)) if qty > 1 else -1
                        invs.append({**inv, "invoice_no": inv_no.replace("INV-", "CM-"), "document_type": "CREDIT",
                                     "invoice_date": rd.isoformat(), "quantity": rq,
                                     "line_amount": f"{round(rq * price * (1 - disc / 100), 2):.2f}",
                                     "shipment_no": "", "posted_at": ts(rd, r), "_d": rd})
        out = {"sales_header": [x for x in headers if x["_d"] <= through],
               "sales_line": [x for x in lines if x["_d"] <= through],
               "shipment": [x for x in ships if x["_d"] <= through],
               "invoice_line": [x for x in invs if x["_d"] <= through]}
        self._erp_cache, self._erp_through = out, through
        return out

    # ---- production ---------------------------------------------------------
    def production(self, through: date) -> dict:
        batches, ingredients, runs, costs = [], [], [], []
        bad_done = self.clean
        for d in daterange(self.start, through):
            if d.weekday() >= 5:
                continue
            r = rng_for(self.seed, "prod", d)
            for n in range(r.choices([0, 1, 2, 3], weights=[10, 40, 35, 15])[0]):
                cands = [b for b in BRANDS if self.brand_active(b[0], d, lead_days=35)]
                b = r.choices(cands, weights=[c[5] for c in cands])[0]
                code, lager = b[0], b[8]
                planned = r.choice([60, 60, 30]) if code in FLAGSHIPS else r.choice([30, 30, 15])
                actual = round(planned * r.uniform(0.91, 0.985), 1)
                pkg_date = d + timedelta(days=r.randint(28, 35) if lager else r.randint(14, 24))
                batch_id = f"B{d:%y%m%d}-{n + 1}"
                brewer = r.choice(PEOPLE)
                base = {"batch_id": batch_id, "brand_code": code, "recipe_version": f"v{r.randint(1, 4)}",
                        "brew_date": d.isoformat(), "tank_id": r.choice(TANKS), "planned_volume_bbl": planned,
                        "actual_volume_bbl": actual, "brewer": brewer}
                batches.append({**base, "status": "FERMENTING", "package_date": None,
                                "updated_at": ts(d, r, 6, 15), "_d": d})
                ing_total = 0.0
                for name, uom, per_bbl, unit_cost in (("Pale malt", "LB", r.uniform(48, 62), 0.62),
                                                      ("Specialty malt", "LB", r.uniform(4, 12), 0.95),
                                                      ("Hops", "LB", r.uniform(0.5, 3.2), 14.5),
                                                      ("Yeast", "PITCH", 1 / planned, 185.0)):
                    if b[3] != "BEER" and "malt" in name:
                        name, per_bbl, unit_cost = name.replace("malt", "sugar base"), per_bbl * 0.5, 0.48
                    qty = round(per_bbl * planned, 2)
                    cost = round(qty * unit_cost, 2)
                    ing_total += cost
                    ingredients.append({"batch_id": batch_id, "ingredient": name, "quantity": qty, "uom": uom,
                                        "cost": cost, "lot_no": f"L{r.randint(10000, 99999)}",
                                        "recorded_at": ts(d, r, 6, 15), "_d": d})
                batches.append({**base, "status": "PACKAGED", "package_date": pkg_date.isoformat(),
                                "updated_at": ts(pkg_date, r, 6, 15), "_d": pkg_date})
                out_bbl = actual * r.uniform(0.92, 0.975)
                items = self.items_by_brand[code]
                shares = [r.uniform(0.5, 1.5) for _ in items]
                make_bad = (not bad_done) and pkg_date >= self.bf - timedelta(days=45) and pkg_date <= self.bf
                for k, (item_no, share) in enumerate(zip(items, shares), start=1):
                    gal = PACKAGES[self.items[item_no]["package_code"]][3]
                    vol = out_bbl * share / sum(shares)
                    if make_bad and k == 1:  # keying error: far more volume packaged than was brewed
                        vol = actual * 1.4 - out_bbl * (1 - share / sum(shares))
                    units = int(vol * GAL_PER_BBL / gal)
                    runs.append({"run_id": f"PR-{batch_id[1:]}-{k}", "batch_id": batch_id, "item_no": item_no,
                                 "run_date": pkg_date.isoformat(),
                                 "line": "KEG-LINE" if self.items[item_no]["base_uom"] == "KEG" else "CAN-LINE",
                                 "units_packaged": units, "volume_bbl": round(units * gal / GAL_PER_BBL, 2),
                                 "operator": r.choice(PEOPLE), "recorded_at": ts(pkg_date, r, 6, 15), "_d": pkg_date})
                    if make_bad and k == 1:
                        # Act 2: the brewery spots the keying error and re-sends the run, corrected, in the
                        # next day's production feed (same run_id, later recorded_at). Staging keeps the newest
                        # version, so the yield error clears once it is loaded. No random draws here, so the
                        # rest of the generated data is unchanged.
                        fix_day = self.bf + timedelta(days=1)
                        good_units = int(out_bbl * share / sum(shares) * GAL_PER_BBL / gal)
                        runs.append({**runs[-1], "units_packaged": good_units,
                                     "volume_bbl": round(good_units * gal / GAL_PER_BBL, 2),
                                     "recorded_at": f"{fix_day.isoformat()} 09:12:00", "_d": fix_day})
                bad_done = bad_done or make_bad
                for ctype, amount in (("INGREDIENTS", ing_total), ("LABOR", planned * r.uniform(14, 20)),
                                      ("OVERHEAD", planned * r.uniform(22, 30)),
                                      ("PACKAGING_MATERIALS", planned * r.uniform(28, 46))):
                    costs.append({"batch_id": batch_id, "cost_type": ctype, "amount": round(amount, 2),
                                  "posted_date": pkg_date.isoformat(), "_d": pkg_date})
        f = lambda rows: [x for x in rows if x["_d"] <= through]
        return {"batches": f(batches), "ingredient_usage": f(ingredients),
                "packaging_runs": f(runs), "batch_costs": f(costs)}

    # ---- taproom POS --------------------------------------------------------
    def pos_day(self, d: date) -> list[dict]:
        if d.weekday() == 0:  # closed Mondays
            return []
        r = rng_for(self.seed, "pos", d)
        base = {1: 45, 2: 55, 3: 70, 4: 130, 5: 160, 6: 90}[d.weekday()]
        n = int(base * SEASON[d.month - 1] * r.uniform(0.85, 1.15))
        open_hr = 12 if d.weekday() >= 4 else 15
        draft = [b for b in BRANDS if b[3] != "NA" and self.brand_active(b[0], d)]
        cans = [b for b in BRANDS if self.brand_active(b[0], d)]
        txns = []
        for i in range(1, n + 1):
            minute = r.randint(open_hr * 60, 22 * 60 + 45)
            created = datetime(d.year, d.month, d.day, minute // 60, minute % 60, r.randint(0, 59))
            mug_club = r.random() < 0.08
            lines, subtotal = [], 0.0
            for ln in range(1, r.choices([1, 2, 3, 4], weights=[40, 35, 17, 8])[0] + 1):
                kind = r.choices(["DRAFT", "TOGO", "MERCH"], weights=[80, 12, 8])[0]
                if kind == "DRAFT":
                    b = r.choices(draft, weights=[x[5] for x in draft])[0]
                    var, price = r.choices([("16 oz", 7.0), ("10 oz", 5.0), ("Flight", 12.0)], weights=[70, 20, 10])[0]
                    if b[0] == "DWD" and var == "16 oz":
                        var, price = "10 oz", 6.0
                    code, name, cat = f"DRAFT-{b[0]}", b[1], "Draft"
                elif kind == "TOGO":
                    b = r.choices(cans, weights=[x[5] for x in cans])[0]
                    sixteen = b[3] == "BEER"
                    var, price = ("4-pack 16 oz", 14.0) if sixteen else ("6-pack 12 oz", 12.0)
                    code, name, cat = f"TOGO-{b[0]}-{'4x16' if sixteen else '6x12'}", b[1], "To-Go"
                else:
                    code, name, var, price = r.choice([("MERCH-GLASS", "Logo Pint Glass", "", 8.0),
                                                       ("MERCH-TEE", "Larkspur Tee", "L", 25.0),
                                                       ("MERCH-HAT", "Pine Trucker Hat", "", 22.0)])
                    cat = "Merch"
                qty = r.choices([1, 2, 3], weights=[75, 20, 5])[0]
                disc = round(qty * price * 0.10, 2) if mug_club and kind == "DRAFT" else 0.0
                gross = round(qty * price, 2)
                subtotal += gross - disc
                lines.append({"line_id": ln, "item_code": code, "item_name": name, "category": cat,
                              "variation": var, "quantity": qty, "unit_price": price,
                              "discount_amount": disc, "line_total": round(gross - disc, 2)})
            pay = r.choices(["CARD", "CASH", "GIFT_CARD"], weights=[85, 10, 5])[0]
            tax = round(subtotal * 0.09, 2)
            tip = round(subtotal * r.uniform(0.15, 0.25), 2) if pay == "CARD" and r.random() < 0.6 else 0.0
            txns.append({"transaction_id": "", "created_at": created.isoformat(),
                         "location": "TAPROOM", "register": r.choice(["TAP-1", "TAP-1", "TAP-2"]),
                         "employee": r.choice(PEOPLE), "status": "REFUNDED" if r.random() < 0.004 else "COMPLETED",
                         "payment_type": pay, "customer_program": "MUG_CLUB" if mug_club else None,
                         "subtotal": round(subtotal, 2), "tax": tax, "tip": tip,
                         "total": round(subtotal + tax + tip, 2), "line_items": lines})
        txns.sort(key=lambda t: t["created_at"])
        for i, t in enumerate(txns, start=1):
            t["transaction_id"] = f"T-{d:%Y%m%d}-{i:04d}"
        return txns

    def pos_resend_days(self) -> list[date]:
        """History days whose file was sent twice (duplicate transactions land in bronze)."""
        if self.clean:
            return []
        r = rng_for(self.seed, "pos_resend")
        span = (self.bf - self.start).days
        days = [self.start + timedelta(days=r.randint(30, span - 30)) for _ in range(2)]
        return [d if d.weekday() != 0 else d + timedelta(days=1) for d in days]

    # ---- distributor depletion reports --------------------------------------
    def dist_reports(self, through: date) -> list[dict]:
        """One entry per (distributor, month) whose report has been received by `through`."""
        inv = self.erp(through)["invoice_line"]
        reports = []
        last_full = month_key(self.bf.replace(day=1) - timedelta(days=1))
        months = sorted({month_key(d) for d in daterange(self.start, through)})
        for code, cust_no, _, _, state, _, _, lag in DISTRIBUTORS:
            for ym in months:
                first, last = month_bounds(ym)
                received = last + timedelta(days=lag)
                if not self.clean and code == "HAWKEYE" and ym == last_full:
                    received = self.bf + timedelta(days=3)  # the late file
                if received > through or last > through:
                    continue
                shipped = defaultdict(int)
                for x in inv:
                    if x["customer_no"] == cust_no and x["document_type"] == "INVOICE" and ym == x["invoice_date"][:7]:
                        shipped[x["item_no"]] += x["quantity"]
                r = rng_for(self.seed, "dist", code, ym)
                accts = self.dist_accounts[code]
                rows = []
                for item_no in sorted(shipped):
                    it = self.items[item_no]
                    keg = it["base_uom"] == "KEG"
                    # distributors sell through most, never more, of what they received that month
                    units = int(round(shipped[item_no] * r.uniform(0.85, 0.99)))
                    pool = [a for a in accts if a["on"] == keg or r.random() < 0.15]
                    k = max(1, min(len(pool), units, r.randint(6, 25)))
                    picked = r.sample(pool, k)
                    w = [a["weight"] for a in picked]
                    alloc = [int(units * x / sum(w)) for x in w]
                    alloc[0] += units - sum(alloc)
                    for a, q in zip(picked, alloc):
                        if q > 0:
                            rows.append({"acct": a, "item_no": item_no, "sku": self.dist_sku[code][item_no],
                                         "desc": it["description"], "qty": q, "keg": keg})
                reports.append({"dist": code, "month": ym, "received": received, "rows": rows})
        return reports


# ----------------------------------------------------------------------------
# file layouts
# ----------------------------------------------------------------------------
ERP_FIELDS = {
    "customer": ["customer_no", "customer_name", "customer_type", "address_line1", "city", "state", "postal_code",
                 "price_tier", "territory", "salesperson", "payment_terms", "created_date", "updated_at"],
    "item": ["item_no", "description", "brand_code", "brand_name", "style", "category", "abv", "package_code",
             "base_uom", "standard_cost", "status", "created_date", "updated_at"],
    "price_list": ["item_no", "price_tier", "unit_price", "currency", "starting_date", "ending_date", "updated_at"],
    "sales_header": ["order_no", "customer_no", "order_date", "requested_ship_date", "status", "order_source", "updated_at"],
    "sales_line": ["order_no", "line_no", "item_no", "quantity", "unit_of_measure", "unit_price", "line_discount_pct"],
    "shipment": ["shipment_no", "order_no", "customer_no", "ship_date", "ship_from", "carrier", "posted_at"],
    "invoice_line": ["invoice_no", "invoice_line_no", "document_type", "invoice_date", "order_no", "order_line_no",
                     "customer_no", "item_no", "quantity", "unit_price", "line_discount_pct", "line_amount",
                     "shipment_no", "posted_at"],
}


def write_dist_report(out: Path, rep: dict) -> tuple[str, int]:
    code, ym, rows = rep["dist"], rep["month"], rep["rows"]
    folder = out / "dist" / rep["received"].isoformat()
    _, last = month_bounds(ym)
    if code == "NORTHLAND":  # one row per account/item, separate Cases and Kegs columns
        path = folder / f"northland_depletions_{ym}.csv"
        data = [{"Period": ym, "Acct #": x["acct"]["id"], "Account Name": x["acct"]["name"],
                 "City": x["acct"]["city"], "ST": x["acct"]["state"],
                 "Premise": "ON" if x["acct"]["on"] else "OFF", "Supplier Item": x["sku"], "Item Desc": x["desc"],
                 "Cases": 0 if x["keg"] else x["qty"], "Kegs": x["qty"] if x["keg"] else 0} for x in rows]
        n = write_csv(path, list(data[0].keys()) if data else ["Period"], data)
    elif code == "BADGER":  # quantity + unit of measure, US date format
        path = folder / f"badger_ridge_sales_{ym.replace('-', '')}.csv"
        data = [{"month_ending": f"{last:%m/%d/%Y}", "retailer_id": x["acct"]["id"],
                 "retailer": x["acct"]["name"].upper(), "city": x["acct"]["city"], "state": x["acct"]["state"],
                 "product_code": x["sku"], "product": x["desc"], "qty": x["qty"],
                 "uom": "KG" if x["keg"] else "CS"} for x in rows]
        n = write_csv(path, list(data[0].keys()) if data else ["month_ending"], data)
    else:  # HAWKEYE: pipe-delimited text
        path = folder / f"HPW_DEPL_{ym.replace('-', '')}.txt"
        data = [{"DIST_ID": "HPW", "RPT_YYYYMM": ym.replace("-", ""), "OUTLET_CD": x["acct"]["id"],
                 "OUTLET_NM": x["acct"]["name"], "OUTLET_CITY": x["acct"]["city"],
                 "PREMISE": "ON-PREM" if x["acct"]["on"] else "OFF-PREM", "BRAND_PKG": x["sku"],
                 "PKG_DESC": x["desc"], "UNITS_SOLD": x["qty"]} for x in rows]
        n = write_csv(path, list(data[0].keys()) if data else ["DIST_ID"], data, delimiter="|")
    return str(path.relative_to(out)), n


def emit(world: World, out: Path, day: date, full: bool) -> dict:
    """Write the full extract through `day` (full=True) or only that day's changes."""
    counts: dict[str, int] = {}
    keep = (lambda x: x["_d"] <= day) if full else (lambda x: x["_d"] == day)
    folder = day.isoformat()

    # ERP
    erp = world.erp(day)
    if full:
        customers = [world.customer_at(no, day) for no in sorted(world.customer_versions)]
        latest = {}
        for h in erp["sales_header"]:
            latest[h["order_no"]] = h
        headers = list(latest.values())
        items = [it for it in world.items.values() if it["_created"] <= day]
        prices = world.price_rows(day)
    else:
        customers = [v for vs in world.customer_versions.values() for v in vs if v["_d"] == day]
        headers = [h for h in erp["sales_header"] if h["_d"] == day]
        items = [it for it in world.items.values() if it["_created"] == day]
        prices = world.price_rows(day, only_day=day)
    tables = {"customer": customers, "item": items, "price_list": prices, "sales_header": headers,
              "sales_line": list(filter(keep, erp["sales_line"])), "shipment": list(filter(keep, erp["shipment"])),
              "invoice_line": list(filter(keep, erp["invoice_line"]))}
    for name, rows in tables.items():
        counts[f"erp/{folder}/{name}.csv"] = write_csv(out / "erp" / folder / f"{name}.csv", ERP_FIELDS[name], rows)

    # production
    prod = world.production(day)
    if full:
        latest = {}
        for b in prod["batches"]:
            latest[b["batch_id"]] = b
        prod["batches"] = list(latest.values())
    for name, rows in prod.items():
        rows = rows if full and name == "batches" else list(filter(keep, rows))
        counts[f"prod/{folder}/{name}.json"] = write_jsonl(out / "prod" / folder / f"{name}.json", rows)

    # POS
    if full:
        by_month = defaultdict(list)
        for d in daterange(world.start, day):
            by_month[month_key(d)].extend(world.pos_day(d))
        for d in world.pos_resend_days():
            by_month[month_key(d)].extend(world.pos_day(d))
        for ym, rows in by_month.items():
            counts[f"pos/{folder}/transactions_{ym}.json"] = write_jsonl(out / "pos" / folder / f"transactions_{ym}.json", rows)
    else:
        rows = world.pos_day(day)
        if rows:
            counts[f"pos/{folder}/transactions_{folder}.json"] = write_jsonl(out / "pos" / folder / f"transactions_{folder}.json", rows)
        if not world.clean and day == world.bf + timedelta(days=1):  # yesterday's file arrives again
            prev = world.bf if world.bf.weekday() != 0 else world.bf - timedelta(days=1)
            name = f"pos/{folder}/transactions_{prev.isoformat()}_resend.json"
            counts[name] = write_jsonl(out / name, world.pos_day(prev))

    # distributor reports
    for rep in world.dist_reports(day):
        if (full and rep["received"] <= day) or rep["received"] == day:
            rel, n = write_dist_report(out, rep)
            counts[rel] = n
    return counts


# ----------------------------------------------------------------------------
# seeds (business-owned reference data that lives in git under dbt/seeds)
# ----------------------------------------------------------------------------
def write_seeds(world: World, seeds_dir: Path) -> dict:
    counts = {}
    xref, accts = [], []
    for code, cust_no, name, _, state, *_ in DISTRIBUTORS:
        for item_no, sku in world.dist_sku[code].items():
            unmapped = (not world.clean and world.items[item_no]["brand_code"] == NEW_BRAND
                        and code in ("NORTHLAND", "HAWKEYE"))
            if not unmapped:
                xref.append({"distributor_code": code, "distributor_sku": sku, "item_no": item_no})
        for a in world.dist_accounts[code]:
            accts.append({"distributor_code": code, "distributor_account_id": a["id"],
                          "account_name": a["name"], "premise_type": "ON_PREMISE" if a["on"] else "OFF_PREMISE",
                          "city": a["city"], "state_code": a["state"]})
    counts["ref_distributor_sku_xref.csv"] = write_csv(seeds_dir / "ref_distributor_sku_xref.csv",
                                                       ["distributor_code", "distributor_sku", "item_no"], xref)
    counts["ref_distributor_account_xref.csv"] = write_csv(
        seeds_dir / "ref_distributor_account_xref.csv",
        ["distributor_code", "distributor_account_id", "account_name", "premise_type", "city", "state_code"], accts)
    counts["ref_distributor.csv"] = write_csv(
        seeds_dir / "ref_distributor.csv", ["distributor_code", "distributor_name", "erp_customer_no", "state_code"],
        [{"distributor_code": c, "distributor_name": n, "erp_customer_no": no, "state_code": s}
         for c, no, n, _, s, *_ in DISTRIBUTORS])
    uom = [{"package_code": p, "description": v[1], "unit_type": v[2], "gallons": v[3],
            "barrels": round(v[3] / GAL_PER_BBL, 5), "case_equivalents": round(v[3] / 2.25, 4)}
           for p, v in PACKAGES.items()]
    for p, desc, gal in (("POUR-16", "16 oz draft pour", 0.125), ("POUR-10", "10 oz draft pour", 0.078125),
                         ("FLIGHT", "Flight, 4 x 5 oz", 0.15625), ("PK-4x16", "4-pack 16 oz cans", 0.5),
                         ("PK-6x12", "6-pack 12 oz cans", 0.5625)):
        uom.append({"package_code": p, "description": desc, "unit_type": "TAPROOM", "gallons": gal,
                    "barrels": round(gal / GAL_PER_BBL, 5), "case_equivalents": round(gal / 2.25, 4)})
    counts["ref_uom_conversion.csv"] = write_csv(seeds_dir / "ref_uom_conversion.csv", list(uom[0].keys()), uom)
    states = []
    for code, name, region, variants in (("MN", "Minnesota", "Minnesota", ["MN", "MINNESOTA", "MINN."]),
                                         ("WI", "Wisconsin", "Wisconsin", ["WI", "WISCONSIN", "WIS."]),
                                         ("IA", "Iowa", "Iowa", ["IA", "IOWA"]),
                                         ("ND", "North Dakota", "Dakotas", ["ND", "NORTH DAKOTA", "N.D."])):
        states += [{"state_text": v, "state_code": code, "state_name": name, "sales_region": region} for v in variants]
    counts["ref_state_region.csv"] = write_csv(seeds_dir / "ref_state_region.csv", list(states[0].keys()), states)
    excise = [("MN", 4.60), ("WI", 2.00), ("IA", 5.89)] + ([("ND", 4.96)] if world.clean else [])
    counts["ref_excise_tax_rate.csv"] = write_csv(
        seeds_dir / "ref_excise_tax_rate.csv", ["state_code", "rate_per_barrel", "effective_from"],
        [{"state_code": s, "rate_per_barrel": r, "effective_from": "2025-01-01"} for s, r in excise])
    counts["ref_channel.csv"] = write_csv(
        seeds_dir / "ref_channel.csv", ["source_system", "customer_type", "channel", "channel_group"],
        [{"source_system": "ERP", "customer_type": "DISTRIBUTOR", "channel": "Wholesale", "channel_group": "Three-tier"},
         {"source_system": "ERP", "customer_type": "RETAIL", "channel": "Direct - Off premise", "channel_group": "Self-distribution"},
         {"source_system": "ERP", "customer_type": "ON_PREMISE", "channel": "Direct - On premise", "channel_group": "Self-distribution"},
         {"source_system": "ERP", "customer_type": "EVENT", "channel": "Events", "channel_group": "Self-distribution"},
         {"source_system": "POS", "customer_type": "TAPROOM", "channel": "Taproom", "channel_group": "Taproom"}])
    cal = []
    fy_start = date(2024, 12, 29)  # a Sunday; each fiscal year is 52 weeks in a 4-4-5 pattern
    for fy in range(2025, 2028):
        cur = fy_start + timedelta(weeks=52 * (fy - 2025))
        for p in range(1, 13):
            weeks = 5 if p % 3 == 0 else 4
            end = cur + timedelta(weeks=weeks) - timedelta(days=1)
            cal.append({"fiscal_year": fy, "fiscal_quarter": (p - 1) // 3 + 1, "fiscal_period": p,
                        "period_name": f"FY{fy}-P{p:02d}", "start_date": cur.isoformat(),
                        "end_date": end.isoformat(), "weeks_in_period": weeks})
            cur = end + timedelta(days=1)
    counts["ref_fiscal_calendar_445.csv"] = write_csv(seeds_dir / "ref_fiscal_calendar_445.csv", list(cal[0].keys()), cal)
    return counts


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backfill", help="write the full history extract")
    b.add_argument("--business-date", default=DEFAULT_BUSINESS_DATE)
    b.add_argument("--start", default=DEFAULT_START)
    b.add_argument("--seed", type=int, default=DEFAULT_SEED)
    b.add_argument("--clean", action="store_true", help="omit the planted data-quality defects")
    b.add_argument("--write-seeds", action="store_true", help="also (re)write dbt seed CSVs; overwrites manual edits")
    b.add_argument("--seeds-dir", default="dbt/seeds")
    b.add_argument("--force", action="store_true", help="replace an existing landing zone")
    a = sub.add_parser("advance", help="write the next business day(s) of incremental files")
    a.add_argument("--days", type=int, default=1)
    for p in (b, a):
        p.add_argument("--out", default="data/landing")
    args = ap.parse_args(argv)
    out = Path(args.out)
    state_path = out / "_state.json"

    if args.cmd == "backfill":
        if state_path.exists() and not args.force:
            print(json.dumps({"ok": False, "error": f"{state_path} exists; pass --force to regenerate"}))
            return 1
        for system in ("erp", "prod", "pos", "dist"):
            shutil.rmtree(out / system, ignore_errors=True)
        state = {"seed": args.seed, "start_date": args.start, "backfill_date": args.business_date,
                 "business_date": args.business_date, "clean": args.clean}
        world = World(args.seed, date.fromisoformat(args.start), date.fromisoformat(args.business_date), args.clean)
        counts = emit(world, out, world.bf, full=True)
        seeds = write_seeds(world, Path(args.seeds_dir)) if args.write_seeds else {}
        state_path.write_text(json.dumps(state, indent=2))
        summary = {"ok": True, "cmd": "backfill", "business_date": args.business_date, "clean": args.clean,
                   "files": len(counts), "rows": sum(counts.values()), "seeds": seeds}
    else:
        if not state_path.exists():
            print(json.dumps({"ok": False, "error": "no landing zone yet; run `backfill` first"}))
            return 1
        state = json.loads(state_path.read_text())
        world = World(state["seed"], date.fromisoformat(state["start_date"]),
                      date.fromisoformat(state["backfill_date"]), state["clean"])
        day = date.fromisoformat(state["business_date"])
        counts = {}
        for _ in range(args.days):
            day += timedelta(days=1)
            counts.update(emit(world, out, day, full=False))
        state["business_date"] = day.isoformat()
        state_path.write_text(json.dumps(state, indent=2))
        summary = {"ok": True, "cmd": "advance", "business_date": day.isoformat(),
                   "files": {k: v for k, v in counts.items() if v}, "rows": sum(counts.values())}
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
