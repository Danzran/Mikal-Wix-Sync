#!/usr/bin/env python3
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import argparse

RE_EMPLOYEE = re.compile(r"Employee#:\s*(\d+)\s+(.+?)(?:\s{2,}.*)?$")
RE_BOOKED = re.compile(r"%Booked")
RE_APPT_ROW = re.compile(
    r"(?P<date>\d{2}/\d{2}/\d{2})\s+"
    r"(?P<time>\d{1,2}:\d{2}[ap])\s+"
    r"(?P<cust>\d+)\s+"
    r"(?P<name>[A-Z ,.'-]+?)\s+\d+\s+"
    r"(?P<service>.+?)\s+"
    r"(?P<slots>\d+)\s+\d+\s+\S+$"
)

def parse_lst(path: Path):
    with path.open("r", errors="ignore") as f:
        lines = [l.rstrip("\n") for l in f]

    appointments = []
    current_employee_no = None
    current_employee_name = None
    in_employee = False
    in_detail_block = False

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()

        # detect employee
        m = RE_EMPLOYEE.search(line)
        if m:
            in_employee = True
            in_detail_block = False
            current_employee_no = m.group(1).strip()
            current_employee_name = m.group(2).strip()
            i += 1
            continue

        if in_employee:
            if "End of Employee Appointments" in line:
                in_employee = False
                in_detail_block = False
                current_employee_no = None
                current_employee_name = None
                i += 1
                continue

            if RE_BOOKED.search(line):
                in_detail_block = True
                i += 1
                continue

            if in_detail_block:
                # skip separators and empty lines and continuation lines
                if not line or line.strip().startswith("(") or set(line.strip()) == {"-"}:
                    i += 1
                    continue

                mrow = RE_APPT_ROW.match(line)
                if mrow:
                    date_s = mrow.group("date")
                    time_s = mrow.group("time")
                    cust = mrow.group("cust")
                    name = mrow.group("name").strip().title()
                    service = mrow.group("service").strip()
                    slots = int(mrow.group("slots"))

                    # time parsing
                    t_fixed = time_s[:-1] + ("AM" if time_s.endswith("a") else "PM")
                    start_dt = datetime.strptime(date_s + " " + t_fixed, "%m/%d/%y %I:%M%p")
                    end_dt = start_dt + timedelta(minutes=15 * slots)

                    appointments.append(
                        {
                            "employee_number": current_employee_no,
                            "employee_name": current_employee_name,
                            "customer_number": cust,
                            "client_name": name,
                            "service": service,
                            "slots": slots,
                            "start_iso": start_dt.isoformat(),
                            "end_iso": end_dt.isoformat(),
                            "date": date_s,
                            "start_time": start_dt.strftime("%H:%M"),
                            "end_time": end_dt.strftime("%H:%M"),
                            "raw_line": line.strip(),
                        }
                    )
        i += 1
    return appointments

def write_sqlite(appointments, db_path: Path):
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS appointments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_number TEXT,
        employee_name TEXT,
        customer_number TEXT,
        client_name TEXT,
        service TEXT,
        slots INTEGER,
        start_iso TEXT,
        end_iso TEXT,
        date TEXT,
        start_time TEXT,
        end_time TEXT,
        raw_line TEXT
    );
    """
    )
    for ap in appointments:
        cur.execute(
            """INSERT INTO appointments
            (employee_number, employee_name, customer_number, client_name, service, slots, start_iso, end_iso, date, start_time, end_time, raw_line)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ap["employee_number"],
                ap["employee_name"],
                ap["customer_number"],
                ap["client_name"],
                ap["service"],
                ap["slots"],
                ap["start_iso"],
                ap["end_iso"],
                ap["date"],
                ap["start_time"],
                ap["end_time"],
                ap["raw_line"],
            ),
        )
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Parse LST employee appointment listings into sqlite.")
    parser.add_argument("--lst", required=True, help="Path to the .LST file")
    parser.add_argument("--db", required=True, help="Path to output sqlite database (will be replaced)")
    args = parser.parse_args()

    lst_path = Path(args.lst)
    db_path = Path(args.db)

    appts = parse_lst(lst_path)
    write_sqlite(appts, db_path)
    print(f"parsed {len(appts)} appointments into {db_path}")

if __name__ == "__main__":
    main()
