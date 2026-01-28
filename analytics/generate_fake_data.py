"""
Generate synthetic LMS analytics data (CSV) based on the agreed schema.
Uses only Python standard library (no external deps).

Schema covered:
  - user_dim.csv
  - course_dim.csv
  - enrol_fact.csv
  - grade_fact.csv
  - submission_fact.csv
  - event_log_staging.csv
  - daily_course_kpi.csv
  - grade_dist.csv
  - submission_latency.csv
  - user_engagement.csv

Usage (Windows PowerShell):
  python LMS/analytics/generate_fake_data.py --users 200 --courses 5 --days 30
CSV files will be written into LMS/db/sample_data/.
"""

from __future__ import annotations
import argparse
import csv
import datetime as dt
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).resolve().parent.parent / "db" / "sample_data"


def rand_name() -> str:
    first = random.choice(
        ["Alex", "Sam", "Taylor", "Morgan", "Jordan", "Jamie", "Dana", "Cody", "Riley", "Kris"]
    )
    last = random.choice(
        ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Do", "Ly", "Bui", "Vo"]
    )
    return f"{first} {last}"


def rand_role() -> str:
    return random.choices(["student", "teacher", "manager"], weights=[0.8, 0.18, 0.02])[0]


def rand_course_name(idx: int) -> str:
    topics = ["Data", "AI", "Cloud", "Security", "DevOps", "Backend", "Frontend", "Mobile"]
    return f"{random.choice(topics)} Bootcamp {idx}"


def generate_users(n: int) -> List["User"]:
    users = []
    for i in range(1, n + 1):
        role = rand_role()
        users.append(
            User(
                id=i,
                moodle_userid=1000 + i,
                name=rand_name(),
                role=role,
                org=random.choice(["OrgA", "OrgB", "OrgC"]),
                created_at=dt.datetime.now() - dt.timedelta(days=random.randint(10, 200)),
            )
        )
    return users


def generate_courses(n: int) -> List["Course"]:
    courses = []
    for i in range(1, n + 1):
        courses.append(
            Course(
                id=i,
                moodle_courseid=2000 + i,
                fullname=rand_course_name(i),
                category=random.choice(["Tech", "Biz", "SoftSkills"]),
                startdate=dt.date.today() - dt.timedelta(days=random.randint(20, 120)),
            )
        )
    return courses


def generate_enrol(users, courses):
    enrolments = []
    for u in users:
        for c in courses:
            prob = 0.35 if u.role in ("teacher", "manager") else 0.6
            if random.random() < prob:
                enrol_time = dt.datetime.combine(
                    c.startdate - dt.timedelta(days=random.randint(1, 10)),
                    dt.time(hour=random.randint(8, 18)),
                )
                enrol_method = random.choice(["manual", "self", "cohort"])
                enrolments.append((u.id, c.id, enrol_method, enrol_time))
    return enrolments


def generate_grades(enrolments, days: int):
    grade_facts = []
    for (uid, cid, _m, _t) in enrolments:
        item_count = random.randint(2, 4)
        for item_id in range(item_count):
            score = random.uniform(40, 100)
            graded_at = dt.datetime.now() - dt.timedelta(days=random.randint(0, days))
            grade_facts.append((uid, cid, item_id + 1, round(score, 2), 100.0, graded_at))
    return grade_facts


def generate_submissions(enrolments, days: int):
    submissions = []
    for (uid, cid, _m, enrol_time) in enrolments:
        activity_id = random.randint(1, 8)
        submitted_at = enrol_time + dt.timedelta(days=random.randint(1, days))
        duedate = enrol_time + dt.timedelta(days=random.randint(2, days + 5))
        status = random.choice(["submitted", "graded", "late"])
        submissions.append(
            (uid, cid, activity_id, random.choice(["assignment", "quiz"]), submitted_at, duedate, status)
        )
    return submissions


def generate_events(enrolments, days: int):
    events = []
    actions = ["viewed", "submitted", "graded", "commented", "attempted"]
    modules = ["course", "assign", "quiz", "page", "forum"]
    for (uid, cid, _m, enrol_time) in enrolments:
        for _ in range(random.randint(5, 20)):
            t = enrol_time + dt.timedelta(days=random.randint(0, days), hours=random.randint(0, 23))
            events.append((uid, cid, random.choice(modules), random.choice(actions), int(t.timestamp()), "{}"))
    return events


def aggregate_daily_kpi(events, grades):
    kpi = {}
    for uid, cid, module, action, ts, _ in events:
        d = dt.date.fromtimestamp(ts)
        key = (cid, d)
        kpi.setdefault(key, {"active": set(), "submissions": 0, "completions": 0, "grades": []})
        kpi[key]["active"].add(uid)
        if action == "submitted":
            kpi[key]["submissions"] += 1
        if action == "graded":
            kpi[key]["completions"] += 1
    for uid, cid, item_id, score, maxscore, graded_at in grades:
        d = graded_at.date()
        key = (cid, d)
        kpi.setdefault(key, {"active": set(), "submissions": 0, "completions": 0, "grades": []})
        kpi[key]["grades"].append(score)
    rows = []
    for (cid, day), val in kpi.items():
        avg_grade = sum(val["grades"]) / len(val["grades"]) if val["grades"] else 0
        rows.append((cid, day.isoformat(), len(val["active"]), val["submissions"], val["completions"], round(avg_grade, 2)))
    return rows


def write_csv(path: Path, headers, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


@dataclass
class User:
    id: int
    moodle_userid: int
    name: str
    role: str
    org: str
    created_at: dt.datetime


@dataclass
class Course:
    id: int
    moodle_courseid: int
    fullname: str
    category: str
    startdate: dt.date


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--courses", type=int, default=5)
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()

    random.seed(42)
    ROOT.mkdir(parents=True, exist_ok=True)

    users = generate_users(args.users)
    courses = generate_courses(args.courses)
    enrol = generate_enrol(users, courses)
    grades = generate_grades(enrol, args.days)
    submissions = generate_submissions(enrol, args.days)
    events = generate_events(enrol, args.days)
    kpis = aggregate_daily_kpi(events, grades)

    write_csv(ROOT / "user_dim.csv", ["id", "moodle_userid", "name", "role", "org", "created_at"],
              [(u.id, u.moodle_userid, u.name, u.role, u.org, u.created_at.isoformat()) for u in users])
    write_csv(ROOT / "course_dim.csv", ["id", "moodle_courseid", "fullname", "category", "startdate"],
              [(c.id, c.moodle_courseid, c.fullname, c.category, c.startdate.isoformat()) for c in courses])
    write_csv(ROOT / "enrol_fact.csv", ["user_id", "course_id", "enrol_method", "enrol_time"],
              [(u, c, m, t.isoformat()) for (u, c, m, t) in enrol])
    write_csv(ROOT / "grade_fact.csv", ["user_id", "course_id", "item_id", "score", "maxscore", "graded_at"],
              [(u, c, i, s, mx, t.isoformat()) for (u, c, i, s, mx, t) in grades])
    write_csv(ROOT / "submission_fact.csv", ["user_id", "course_id", "activity_id", "type", "submitted_at", "duedate", "status"],
              [(u, c, a, ty, s.isoformat(), d.isoformat(), st) for (u, c, a, ty, s, d, st) in submissions])
    write_csv(ROOT / "event_log_staging.csv", ["user_id", "course_id", "module", "action", "timecreated", "extra_json"],
              events)
    write_csv(ROOT / "daily_course_kpi.csv", ["course_id", "date", "active_users", "submissions", "completions", "avg_grade"],
              kpis)
    print(f"Sample data generated in: {ROOT}")


if __name__ == "__main__":
    main()
