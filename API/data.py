from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import HTTPException

BASE = Path(__file__).resolve().parents[1] / "db" / "sample_data"


def load_data():
    if not BASE.exists():
        raise HTTPException(status_code=500, detail="sample_data not found")

    user_dim = pd.read_csv(BASE / "user_dim.csv")
    course_dim = pd.read_csv(BASE / "course_dim.csv")
    enrol = pd.read_csv(BASE / "enrol_fact.csv", parse_dates=["enrol_time"])
    grade = pd.read_csv(BASE / "grade_fact.csv", parse_dates=["graded_at"])
    subm = pd.read_csv(
        BASE / "submission_fact.csv", parse_dates=["submitted_at", "duedate"]
    )
    events = pd.read_csv(BASE / "event_log_staging.csv", parse_dates=["timestamp"])
    daily = pd.read_csv(BASE / "daily_course_kpi.csv", parse_dates=["date"])
    return user_dim, course_dim, enrol, grade, subm, events, daily
