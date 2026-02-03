from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, HTTPException

from .data import load_data

router = APIRouter(prefix="/teacher/course", tags=["teacher"])


@router.get("/{course_id}/dashboard")
def teacher_course_dashboard(course_id: int):
    user_dim, course_dim, enrol, grade, subm, events, daily = load_data()

    course_row = course_dim[course_dim.course_id == course_id]
    if course_row.empty:
        raise HTTPException(status_code=404, detail="course_id not found")
    course_name = course_row["fullname"].iat[0]

    today = daily["date"].max().date() if len(daily) else pd.Timestamp.today().date()

    total_students = enrol[enrol.course_id == course_id]["user_id"].nunique()

    g = grade[grade.course_id == course_id]
    avg_grade_pct = (g.score / g.maxscore).mean() * 100 if len(g) else 0

    s = subm[subm.course_id == course_id]
    missing = s[(s.submitted_at.isna()) & (s.duedate.dt.date < today)]
    missing_per_student = (
        missing.groupby("user_id").size().sort_values(ascending=False)
    )

    risk_rows = []
    for uid in enrol[enrol.course_id == course_id]["user_id"].unique():
        stu_grade = g[g.user_id == uid]
        avg_pct = (stu_grade.score / stu_grade.maxscore).mean() * 100 if len(stu_grade) else 0
        grade_risk = 100 - avg_pct

        miss_cnt = missing_per_student.get(uid, 0)
        missing_risk = min(100, miss_cnt * 10)

        last = events[
            (events.course_id == course_id) & (events.user_id == uid)
        ]["timestamp"].max()
        inactivity = (today - last.date()).days if pd.notna(last) else 30
        inactivity_risk = min(100, inactivity / 30 * 100)

        risk = (grade_risk + missing_risk + inactivity_risk) / 3
        risk_rows.append((uid, risk))

    risk_df = (
        pd.DataFrame(risk_rows, columns=["user_id", "risk_pct"])
        .sort_values("risk_pct", ascending=False)
    )
    at_risk_threshold = 60
    at_risk_count = int((risk_df["risk_pct"] > at_risk_threshold).sum())
    at_risk_pct = (at_risk_count / len(risk_df) * 100) if len(risk_df) else 0

    # course_rating.csv is optional in demo data
    avg_rating = 0
    num_ratings = 0
    rating_df = None
    try:
        from .data import BASE  # avoid re-resolving path

        rating_df = pd.read_csv(BASE / "course_rating.csv")
    except Exception:
        rating_df = None

    if rating_df is not None and len(rating_df):
        row = rating_df[rating_df.course_id == course_id]
        if len(row):
            avg_rating = float(row["avg_rating"].iat[0])
            num_ratings = int(row["num_ratings"].iat[0])

    return {
        "course_id": course_id,
        "course_name": course_name,
        "total_students": int(total_students),
        "avg_grade_pct": round(avg_grade_pct, 1),
        "missing_submissions": int(len(missing)),
        "course_rating": {"avg_rating": avg_rating, "num_ratings": num_ratings},
        "at_risk_pct": round(at_risk_pct, 1),
        "at_risk_count": at_risk_count,
        "risk_top": risk_df.head(10).to_dict(orient="records"),
        "missing_per_student": missing_per_student.to_dict(),
    }
