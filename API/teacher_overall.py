from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, HTTPException

from .data import load_data

router = APIRouter(prefix="/teacher", tags=["teacher"])


@router.get("/{teacher_id}/dashboard")
def teacher_overall_dashboard(teacher_id: int):
    user_dim, course_dim, enrol, grade, subm, events, daily = load_data()

    if teacher_id not in set(user_dim[user_dim.role == "teacher"]["user_id"].astype(int)):
        raise HTTPException(status_code=404, detail="teacher_id not found")

    today = daily["date"].max().date() if len(daily) else pd.Timestamp.today().date()

    # courses managed by teacher (demo assumption)
    teacher_courses = enrol[enrol.user_id == teacher_id]["course_id"].unique()

    # students enrolled in teacher's courses (exclude teachers)
    student_ids = set(user_dim[user_dim.role == "student"]["user_id"].astype(int))
    students_in_teacher_courses = [
        uid
        for uid in enrol[enrol.course_id.isin(teacher_courses)]["user_id"].unique()
        if int(uid) in student_ids
    ]

    total_students = int(len(students_in_teacher_courses))
    total_courses = int(len(teacher_courses))

    # inactive students >= 7 days (within teacher courses)
    last_activity = (
        events[events.user_id.isin(students_in_teacher_courses)]
        .groupby("user_id")["timestamp"]
        .max()
        .reset_index()
    )
    inactive_students_7d = int(
        (last_activity["timestamp"].dt.date < today - pd.Timedelta(days=7)).sum()
    )

    # risk per student (simple risk) across teacher courses
    g = grade[grade.course_id.isin(teacher_courses)]
    s = subm[subm.course_id.isin(teacher_courses)]
    missing = s[(s.submitted_at.isna()) & (s.duedate.dt.date < today)]
    missing_per_student = missing.groupby("user_id").size()

    risk_rows = []
    for uid in students_in_teacher_courses:
        stu_grade = g[g.user_id == uid]
        avg_pct = (
            (stu_grade.score / stu_grade.maxscore).mean() * 100 if len(stu_grade) else 0
        )
        grade_risk = 100 - avg_pct

        miss_cnt = missing_per_student.get(uid, 0)
        missing_risk = min(100, miss_cnt * 10)

        last = events[
            (events.user_id == uid) & (events.course_id.isin(teacher_courses))
        ]["timestamp"].max()
        inactivity = (today - last.date()).days if pd.notna(last) else 30
        inactivity_risk = min(100, inactivity / 30 * 100)

        risk = (grade_risk + missing_risk + inactivity_risk) / 3
        risk_rows.append((uid, risk))

    risk_df = (
        pd.DataFrame(risk_rows, columns=["user_id", "risk_pct"])
        .sort_values("risk_pct", ascending=False)
        if risk_rows
        else pd.DataFrame(columns=["user_id", "risk_pct"])
    )

    at_risk_threshold = 60
    at_risk_count = int((risk_df["risk_pct"] > at_risk_threshold).sum())
    at_risk_pct = (at_risk_count / len(risk_df) * 100) if len(risk_df) else 0

    # avg learning hours (proxy) - teacher courses only
    events_tc = events[events.user_id.isin(students_in_teacher_courses)].copy()
    events_tc = events_tc[events_tc.course_id.isin(teacher_courses)]
    events_tc.sort_values(["user_id", "timestamp"], inplace=True)
    events_tc["next_ts"] = events_tc.groupby("user_id")["timestamp"].shift(-1)
    events_tc["session_gap_min"] = (
        (events_tc.next_ts - events_tc.timestamp).dt.total_seconds() / 60
    )
    events_tc = events_tc[events_tc.session_gap_min.between(1, 30)]
    avg_learning_hours = round(events_tc.session_gap_min.mean() / 60, 2)

    # ungraded submissions (overdue + not graded) within teacher courses
    submitted = s[s.submitted_at.notna()].copy()
    submitted["is_overdue"] = submitted["duedate"].dt.date < today
    graded_keys = g[["course_id", "user_id", "item_id"]]
    merged = submitted.merge(
        graded_keys,
        left_on=["course_id", "user_id", "activity_id"],
        right_on=["course_id", "user_id", "item_id"],
        how="left",
        indicator=True,
    )
    overdue_ungraded = merged[(merged.is_overdue) & (merged._merge == "left_only")]
    ungraded_submissions = int(overdue_ungraded.shape[0])

    return {
        "teacher_id": teacher_id,
        "total_students": total_students,
        "total_courses": total_courses,
        "inactive_students_7d": inactive_students_7d,
        "at_risk_pct": round(at_risk_pct, 1),
        "at_risk_count": at_risk_count,
        "avg_learning_hours_teacher_courses": avg_learning_hours,
        "ungraded_submissions": ungraded_submissions,
        "risk_top": risk_df.head(10).to_dict(orient="records"),
    }
