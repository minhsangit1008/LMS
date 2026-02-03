from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from .data import load_data

router = APIRouter(prefix="/student", tags=["student"])


@router.get("/{user_id}/dashboard")
def student_dashboard(
    user_id: int,
    course_id: int = Query(1, gt=0),
):
    user_dim, course_dim, enrol, grade, subm, events, daily = load_data()

    if user_id not in set(user_dim["user_id"].astype(int)):
        raise HTTPException(status_code=404, detail="user_id not found")

    today = daily["date"].max().date() if len(daily) else pd.Timestamp.today().date()

    course_row = course_dim[course_dim.course_id == course_id]
    if course_row.empty:
        raise HTTPException(status_code=404, detail="course_id not found")
    course_name = course_row["fullname"].iat[0]

    total_items = grade[grade.course_id == course_id]["item_id"].nunique()
    completed_items = subm[
        (subm.course_id == course_id)
        & (subm.user_id == user_id)
        & (subm.submitted_at.notna())
    ]["activity_id"].nunique()
    progress_pct = 100 * completed_items / total_items if total_items else 0

    df_grade = grade[(grade.course_id == course_id) & (grade.user_id == user_id)].copy()
    avg_grade_pct = (
        (df_grade["score"] / df_grade["maxscore"]).mean() * 100 if len(df_grade) else 0
    )

    due_soon = subm[
        (subm.user_id == user_id)
        & (subm["duedate"].dt.date >= today)
        & (subm["duedate"].dt.date <= today + pd.Timedelta(days=7))
        & (subm["submitted_at"].isna())
    ].merge(course_dim[["course_id", "fullname"]], on="course_id", how="left")

    missing = subm[
        (subm.user_id == user_id)
        & (subm["duedate"].dt.date < today)
        & (subm["submitted_at"].isna())
    ].merge(course_dim[["course_id", "fullname"]], on="course_id", how="left")

    last_active_ts = events[events.user_id == user_id]["timestamp"].max()
    if pd.isna(last_active_ts):
        days_inactive = None
        last_active = None
    else:
        last_active = last_active_ts.date().isoformat()
        days_inactive = (today - last_active_ts.date()).days

    missing_rows = (
        missing[["fullname", "activity_id", "duedate"]]
        .sort_values("duedate")
        .assign(duedate=lambda d: d["duedate"].dt.date.astype(str))
        .to_dict(orient="records")
    )

    due_soon_rows = (
        due_soon[["fullname", "activity_id", "duedate"]]
        .sort_values("duedate")
        .assign(duedate=lambda d: d["duedate"].dt.date.astype(str))
        .to_dict(orient="records")
    )

    return {
        "course_id": course_id,
        "course_name": course_name,
        "progress_pct": round(progress_pct, 1),
        "avg_grade_pct": round(avg_grade_pct, 1),
        "due_soon_count": int(len(due_soon)),
        "missing_count": int(len(missing)),
        "last_active": last_active,
        "days_inactive": days_inactive,
        "missing_tasks": missing_rows,
        "due_soon_tasks": due_soon_rows,
    }
