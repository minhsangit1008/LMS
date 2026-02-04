from __future__ import annotations

import pandas as pd
from fastapi import APIRouter, HTTPException

from .data import load_data

router = APIRouter(prefix="/mentor", tags=["mentor"])


@router.get("/{mentor_id}/dashboard")
def mentor_dashboard(mentor_id: int):
    user_dim, course_dim, enrol, grade, subm, events, daily = load_data()

    # load mentor-specific datasets
    from pathlib import Path
    from .data import BASE

    idea = pd.read_csv(BASE / "idea_dim.csv")
    match = pd.read_csv(BASE / "mentor_match.csv", parse_dates=["matched_at"])
    pitch = pd.read_csv(BASE / "pitch_readiness.csv")
    mentor_profile = pd.read_csv(BASE / "mentor_profile.csv")

    if mentor_id not in set(mentor_profile["mentor_id"].astype(int)):
        raise HTTPException(status_code=404, detail="mentor_id not found")

    # normalize student column
    if "student_userid" not in idea.columns and "student_id" in idea.columns:
        idea = idea.rename(columns={"student_id": "student_userid"})

    READY_THRESHOLD = 80
    NEW_DAYS = 7

    my_match = match[match.mentor_id == mentor_id].copy()
    my_ideas = idea[idea.idea_id.isin(my_match.idea_id)].copy()

    idea_count = int(my_ideas["idea_id"].nunique())
    mentee_count = int(my_ideas["student_userid"].nunique()) if len(my_ideas) else 0

    pitch_ready_cnt = 0
    if "match_id" in pitch.columns:
        pitch_ready_cnt = int(
            (
                pitch[pitch.match_id.isin(my_match.match_id)]["score_0_100"]
                >= READY_THRESHOLD
            ).sum()
        )

    today = daily["date"].max().date() if len(daily) else pd.Timestamp.today().date()
    matched_recent = my_match[
        my_match["matched_at"] >= (pd.Timestamp(today) - pd.Timedelta(days=NEW_DAYS))
    ]
    new_ideas_cnt = int(matched_recent["idea_id"].nunique())

    return {
        "mentor_id": mentor_id,
        "ideas_managed": idea_count,
        "mentees_managed": mentee_count,
        "deal_ready_ideas": pitch_ready_cnt,
        "new_ideas_last_days": NEW_DAYS,
        "new_ideas_count": new_ideas_cnt,
        "ready_threshold": READY_THRESHOLD,
    }
