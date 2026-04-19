#!/usr/bin/env python3
"""
Real-time sync monitor: shows row-count and data lag between MySQL source
and PostgreSQL target, refreshed every second.

Usage:
    python monitor-sync.py              # refresh every 1s
    python monitor-sync.py --interval 2 # refresh every 2s
    python monitor-sync.py --detail     # also compare latest rows per table

Requirements (auto-installed):  mysql-connector-python  psycopg2-binary  colorama
"""

import argparse
import sys
import time
import os
import subprocess

def ensure(*pkgs):
    for pkg in pkgs:
        try:
            __import__(pkg.replace("-","_").split("[")[0])
        except ImportError:
            print(f"Installing {pkg}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

ensure("mysql-connector-python", "psycopg2-binary", "colorama")

import mysql.connector
import psycopg2
from colorama import init, Fore, Style
init(autoreset=True)

# ── connection params ────────────────────────────────────────────────────────
MYSQL_CFG = dict(host="localhost", port=13307, user="root", password="root", database="sourcedb")
PG_CFG    = dict(host="localhost", port=15433, user="pguser", password="pgpass", dbname="targetdb")

TABLES = ["users", "products", "orders"]

# ── helpers ──────────────────────────────────────────────────────────────────

def mysql_counts(cur):
    out = {}
    for t in TABLES:
        cur.execute(f"SELECT COUNT(*) FROM `{t}`")
        out[t] = cur.fetchone()[0]
    return out

def pg_counts(cur):
    out = {}
    for t in TABLES:
        cur.execute(f'SELECT COUNT(*) FROM "{t}"')
        out[t] = cur.fetchone()[0]
    return out

def mysql_latest(cur, table, n=3):
    """Return last N rows as list of dicts."""
    try:
        cur.execute(f"SELECT * FROM `{table}` ORDER BY id DESC LIMIT {n}")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        return []

def pg_latest(cur, table, n=3):
    try:
        cur.execute(f'SELECT * FROM "{table}" ORDER BY id DESC LIMIT {n}')
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception:
        return []

def color_diff(src, dst):
    diff = src - dst
    if diff == 0:
        return Fore.GREEN + "  ±0"
    elif diff > 0:
        return Fore.YELLOW + f" +{diff}"  # target is behind
    else:
        return Fore.CYAN + f" {diff}"     # target has more (shouldn't normally happen)

def clear():
    os.system("cls" if os.name == "nt" else "clear")

def fmt_row(r):
    return "  " + "  |  ".join(f"{k}={v}" for k, v in list(r.items())[:5])

# ── main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=1.0, help="refresh interval in seconds")
    ap.add_argument("--detail",   action="store_true",     help="show latest 3 rows per table")
    args = ap.parse_args()

    print("Connecting to MySQL and PostgreSQL…")
    my_conn = mysql.connector.connect(**MYSQL_CFG)
    pg_conn = psycopg2.connect(**PG_CFG)
    my_conn.autocommit = True
    pg_conn.autocommit = True
    my_cur = my_conn.cursor()
    pg_cur = pg_conn.cursor()
    print("Connected. Monitoring (Ctrl-C to stop)…\n")
    time.sleep(0.5)

    history = []          # (ts, total_src, total_dst)
    prev_src_total = None

    try:
        while True:
            try:
                src = mysql_counts(my_cur)
                dst = pg_counts(pg_cur)
            except Exception as e:
                # reconnect on transient errors
                try: my_conn.reconnect()
                except: pass
                try: pg_conn = psycopg2.connect(**PG_CFG); pg_cur = pg_conn.cursor(); pg_conn.autocommit = True
                except: pass
                time.sleep(args.interval)
                continue

            ts = time.strftime("%H:%M:%S")
            total_src = sum(src.values())
            total_dst = sum(dst.values())
            history.append((ts, total_src, total_dst))
            if len(history) > 60:
                history.pop(0)

            # throughput (events/sec over last 5 samples)
            tput = ""
            if len(history) >= 2:
                delta_t  = len(history[-5:]) * args.interval
                delta_n  = history[-1][1] - history[-min(6,len(history))][1]
                tput_val = delta_n / delta_t if delta_t else 0
                tput = f"  src rate≈{tput_val:+.1f} rows/s"

            clear()
            print(Fore.CYAN + Style.BRIGHT + f"  ╔══ dbsync monitor  {ts} ══╗" + tput)
            print()
            print(f"  {'TABLE':<14} {'SRC':>8} {'DST':>8} {'LAG':>6}  {'STATUS'}")
            print("  " + "─" * 55)

            all_synced = True
            for t in TABLES:
                s, d = src[t], dst[t]
                lag   = s - d
                diff  = color_diff(s, d)
                if lag == 0:
                    status = Fore.GREEN + "✓ synced"
                elif lag <= 5:
                    status = Fore.YELLOW + f"⚡ {lag} rows behind"
                    all_synced = False
                else:
                    status = Fore.RED + f"✗ {lag} rows behind"
                    all_synced = False
                print(f"  {t:<14} {s:>8} {d:>8} {diff}  {status}")

            print("  " + "─" * 55)
            total_lag = total_src - total_dst
            lag_color = Fore.GREEN if total_lag == 0 else (Fore.YELLOW if total_lag < 10 else Fore.RED)
            print(f"  {'TOTAL':<14} {total_src:>8} {total_dst:>8} " +
                  lag_color + f" {total_lag:+d}" + Style.RESET_ALL)

            # sparkline (last 20 lag values)
            lags = [h[1]-h[2] for h in history[-20:]]
            if lags:
                mx = max(lags) if max(lags) > 0 else 1
                bars = "▁▂▃▄▅▆▇█"
                spark = "".join(bars[min(int(v/mx*7), 7)] if v > 0 else "▁" for v in lags)
                lag_color2 = Fore.GREEN if max(lags) == 0 else Fore.YELLOW
                print(f"\n  Lag trend (last {len(lags)}s): " + lag_color2 + spark)

            if args.detail:
                print()
                for t in TABLES:
                    print(Fore.CYAN + f"  ── {t} (latest 3 rows) ──")
                    src_rows = mysql_latest(my_cur, t)
                    dst_rows = pg_latest(pg_cur, t)
                    src_ids  = {r["id"] for r in src_rows}
                    dst_ids  = {r["id"] for r in dst_rows}
                    for r in src_rows:
                        marker = Fore.GREEN + "SRC " if r["id"] in dst_ids else Fore.RED + "SRC!"
                        print(marker + fmt_row(r))
                    for r in dst_rows:
                        if r["id"] not in src_ids:
                            print(Fore.CYAN + "DST " + fmt_row(r))

            print()
            prev_src_total = total_src
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nMonitor stopped.")
    finally:
        try: my_cur.close(); my_conn.close()
        except: pass
        try: pg_cur.close(); pg_conn.close()
        except: pass

if __name__ == "__main__":
    main()
