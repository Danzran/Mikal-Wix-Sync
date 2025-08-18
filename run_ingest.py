#!/usr/bin/env python3
from pathlib import Path
from busy_blocks_ingest import parse_lst, write_sqlite

if __name__ == "__main__":
    lst = Path("C:/Users/pondu/OneDrive/Desktop/Contour/Mikal-Wix-Sync/LST/sched.LST")  # adjust as needed
    db = Path("C:/Users/pondu/OneDrive/Desktop/Contour/Mikal-Wix-Sync/db/schedule.db")
    appts = parse_lst(lst)
    write_sqlite(appts, db)
    print(f"ok: {len(appts)} appts -> {db}")
