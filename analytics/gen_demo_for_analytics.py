"""
Generate synthetic analytics data that mirrors Moodle core schema (subset needed for KPIs/Charts)
and also writes the simplified CSVs consumed by our FastAPI demo.

Outputs to LMS/db/sample_data:
  Simplified (used by API/UI):
    - user_dim.csv
    - course_dim.csv
    - enrol_fact.csv
    - grade_fact.csv
    - submission_fact.csv
    - event_log_staging.csv
    - daily_course_kpi.csv
    - forum_posts.csv
    - pending_questions.csv
    - course_rating.csv

  Core-like snapshots (columns named like Moodle install.xml):
    - mdl_user.csv
    - mdl_course_categories.csv
    - mdl_course.csv
    - mdl_enrol.csv
    - mdl_user_enrolments.csv
    - mdl_assign.csv
    - mdl_assign_submission.csv
    - mdl_grade_items.csv
    - mdl_grade_grades.csv
    - mdl_course_modules.csv
    - mdl_course_modules_completion.csv
    - mdl_logstore_standard_log.csv
    - mdl_forum.csv
    - mdl_forum_discussions.csv
    - mdl_forum_posts.csv
    - mdl_feedback.csv
    - mdl_feedback_item.csv
    - mdl_feedback_completed.csv
    - mdl_feedback_value.csv
"""
from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

# ---------------- Configuration ---------------- #
SEED = 42
NUM_STUDENTS = 80
NUM_TEACHERS = 5
NUM_MENTORS = 4
NUM_INVESTORS = 2
COURSES = [
    (1, "Backend Internship"),
    (2, "Frontend Internship"),
    (3, "Data Analytics"),
]
CATEGORIES = [
    (1, "Programming"),
    (2, "Business"),
    (3, "Analytics"),
    (4, "Language"),
]
START_DATE = datetime(2025, 9, 1)
END_DATE = datetime(2026, 1, 31)
ITEMS_PER_COURSE = 5  # assignments/quizzes
EVENTS_PER_ACTIVE_DAY = (4, 10)  # min/max events per active user per day
MISSING_SUBMISSION_RATE = 0.10  # keep missing tasks reasonable

random.seed(SEED)
BASE = Path(__file__).resolve().parents[1] / "db" / "sample_data"
BASE.mkdir(parents=True, exist_ok=True)


def daterange(start: datetime, end: datetime):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


# ---------------- Users ---------------- #
mdl_user: List[Dict] = []
user_dim: List[Dict] = []
uid = 1

def add_user(role: str):
    global uid
    firstname = "Student" if role == "student" else "Teacher"
    mdl_user.append(
        {
            "id": uid,
            "auth": "manual",
            "confirmed": 1,
            "deleted": 0,
            "suspended": 0,
            "mnethostid": 1,
            "username": f"{role}{uid:03d}",
            "firstname": firstname,
            "lastname": f"{uid:03d}",
            "email": f"{role}{uid:03d}@example.com",
            "timecreated": int(START_DATE.timestamp()),
        }
    )
    user_dim.append(
        {
            "user_id": uid,
            "fullname": f"{firstname} {uid:03d}",
            "role": role,
            "created_at": START_DATE.date(),
        }
    )
    uid += 1

for _ in range(NUM_STUDENTS):
    add_user("student")
for _ in range(NUM_TEACHERS):
    add_user("teacher")
for _ in range(NUM_MENTORS):
    add_user("mentor")
for _ in range(NUM_INVESTORS):
    add_user("investor")

student_ids = [u["user_id"] for u in user_dim if u["role"] == "student"]
teacher_ids = [u["user_id"] for u in user_dim if u["role"] == "teacher"]
mentor_ids = [u["user_id"] for u in user_dim if u["role"] == "mentor"]
investor_ids = [u["user_id"] for u in user_dim if u["role"] == "investor"]


# ---------------- Categories & Courses ---------------- #
mdl_course_categories = [
    {"id": cid, "name": name, "timecreated": int(START_DATE.timestamp())}
    for cid, name in CATEGORIES
]

course_dim: List[Dict] = []
mdl_course: List[Dict] = []
for cid, fullname in COURSES:
    catid, _ = random.choice(CATEGORIES)
    startdate = START_DATE + timedelta(days=random.randint(0, 20))
    shortname = fullname.replace(" ", "")[:20]
    course_dim.append(
        {
            "course_id": cid,
            "fullname": fullname,
            "category": _,
            "startdate": startdate.date(),
        }
    )
    mdl_course.append(
        {
            "id": cid,
            "category": catid,
            "fullname": fullname,
            "shortname": shortname,
            "startdate": int(startdate.timestamp()),
            "timecreated": int(startdate.timestamp()),
        }
    )


# ---------------- Enrolments ---------------- #
mdl_enrol: List[Dict] = []
mdl_user_enrolments: List[Dict] = []
enrol_fact: List[Dict] = []
enrol_id = 1
ue_id = 1

for cid, _ in COURSES:
    # manual enrol instance
    mdl_enrol.append(
        {
            "id": enrol_id,
            "enrol": "manual",
            "status": 0,
            "courseid": cid,
            "timecreated": int(START_DATE.timestamp()),
        }
    )
    # one teacher
    tid = random.choice(teacher_ids)
    mdl_user_enrolments.append(
        {
            "id": ue_id,
            "enrolid": enrol_id,
            "userid": tid,
            "timestart": int(START_DATE.timestamp()),
        }
    )
    enrol_fact.append(
        {"course_id": cid, "user_id": tid, "enrol_time": START_DATE + timedelta(days=1)}
    )
    ue_id += 1
    # students
    enrolled_students = random.sample(
        student_ids, k=min(len(student_ids), random.randint(20, 35))
    )
    for sid in enrolled_students:
        etime = START_DATE + timedelta(days=random.randint(0, 30))
        mdl_user_enrolments.append(
            {
                "id": ue_id,
                "enrolid": enrol_id,
                "userid": sid,
                "timestart": int(etime.timestamp()),
            }
        )
        enrol_fact.append({"course_id": cid, "user_id": sid, "enrol_time": etime})
        ue_id += 1
    enrol_id += 1

# enrolled students across all courses (for idea generation)
enrolled_student_ids = sorted(
    {e["user_id"] for e in enrol_fact if e["user_id"] in student_ids}
)

# ---------------- Activities (assign as proxy) ---------------- #
mdl_assign: List[Dict] = []
mdl_assign_submission: List[Dict] = []
mdl_grade_items: List[Dict] = []
mdl_grade_grades: List[Dict] = []
mdl_course_modules: List[Dict] = []
mdl_course_modules_completion: List[Dict] = []
mdl_course_modules_viewed: List[Dict] = []
mdl_course_completion_criteria: List[Dict] = []
mdl_course_completion_crit_compl: List[Dict] = []
mdl_course_completions: List[Dict] = []

submission_fact: List[Dict] = []
grade_fact: List[Dict] = []

assign_id = 1
grade_item_id = 1
cmid = 1
sub_id = 1
gg_id = 1
cmc_id = 1
cmv_id = 1
ccc_id = 1
cccc_id = 1
cc_id = 1

course_assignments: Dict[int, List[int]] = {cid: [] for cid, _ in COURSES}
course_modules_by_course: Dict[int, List[int]] = {cid: [] for cid, _ in COURSES}

for cid, _ in COURSES:
    for i in range(ITEMS_PER_COURSE):
        duedate = START_DATE + timedelta(days=20 + i * 25 + random.randint(-3, 3))
        mdl_assign.append(
            {
                "id": assign_id,
                "course": cid,
                "name": f"Assignment {i+1}",
                "duedate": int(duedate.timestamp()),
                "timemodified": int(duedate.timestamp()),
            }
        )
        course_assignments[cid].append(assign_id)
        # course module placeholder (module id for assign often 1 in fresh installs)
        mdl_course_modules.append(
            {
                "id": cmid,
                "course": cid,
                "module": 1,  # not used here
                "instance": assign_id,
                "section": 1,
            }
        )
        course_modules_by_course[cid].append(cmid)
        mdl_grade_items.append(
            {
                "id": grade_item_id,
                "courseid": cid,
                "itemtype": "mod",
                "itemmodule": "assign",
                "iteminstance": assign_id,
                "itemname": f"Assignment {i+1}",
                "grademax": 100,
            }
        )
        # submissions and grades for enrolled students
        course_students = [
            e["user_id"] for e in enrol_fact if e["course_id"] == cid and e["user_id"] in student_ids
        ]
        for sid in course_students:
            missing = random.random() < MISSING_SUBMISSION_RATE
            if missing:
                # no submission record; mark in fact with null submitted_at
                submission_fact.append(
                    {
                        "course_id": cid,
                        "user_id": sid,
                        "activity_id": assign_id,
                        "submitted_at": None,
                        "duedate": duedate,
                    }
                )
            else:
                jitter = random.randint(-24, 48)
                submitted = duedate + timedelta(hours=jitter)
                mdl_assign_submission.append(
                    {
                        "id": sub_id,
                        "assignment": assign_id,
                        "userid": sid,
                        "status": "submitted",
                        "timecreated": int(submitted.timestamp()),
                        "timemodified": int(submitted.timestamp()),
                    }
                )
                submission_fact.append(
                    {
                        "course_id": cid,
                        "user_id": sid,
                        "activity_id": assign_id,
                        "submitted_at": submitted,
                        "duedate": duedate,
                    }
                )
                score = max(45, min(98, random.gauss(78, 10)))
                graded = submitted + timedelta(hours=random.randint(2, 24))
                mdl_grade_grades.append(
                    {
                        "id": gg_id,
                        "itemid": grade_item_id,
                        "userid": sid,
                        "rawgrade": round(score, 2),
                        "finalgrade": round(score, 2),
                        "timemodified": int(graded.timestamp()),
                    }
                )
                grade_fact.append(
                    {
                        "course_id": cid,
                        "user_id": sid,
                        "item_id": assign_id,
                        "score": round(score, 2),
                        "maxscore": 100,
                        "graded_at": graded,
                    }
                )
                # completion
                completionstate = 1 if submitted <= duedate + timedelta(hours=72) else 0
                mdl_course_modules_completion.append(
                    {
                        "id": cmc_id,
                        "coursemoduleid": cmid,
                        "userid": sid,
                        "completionstate": completionstate,
                        "timemodified": int(submitted.timestamp()),
                    }
                )
                cmc_id += 1
                # also mark module viewed (simple proxy)
                if random.random() > 0.2:
                    mdl_course_modules_viewed.append(
                        {
                            "id": cmv_id,
                            "coursemoduleid": cmid,
                            "userid": sid,
                            "timecreated": int(submitted.timestamp()),
                        }
                    )
                    cmv_id += 1
                sub_id += 1
                gg_id += 1
        assign_id += 1
        grade_item_id += 1
        cmid += 1


# ---------------- Events / Logs ---------------- #
mdl_logstore_standard_log: List[Dict] = []
event_log: List[Dict] = []
log_id = 1
for cid, _ in COURSES:
    course_students = [
        e["user_id"] for e in enrol_fact if e["course_id"] == cid and e["user_id"] in student_ids
    ]
    for d in daterange(START_DATE, END_DATE):
        active_count = max(1, int(len(course_students) * random.uniform(0.35, 0.7)))
        active_users = random.sample(course_students, k=min(len(course_students), active_count))
        for uid in active_users:
            events_today = random.randint(*EVENTS_PER_ACTIVE_DAY)
            for _ in range(events_today):
                ts = datetime(d.year, d.month, d.day, random.randint(6, 22), random.randint(0, 59))
                event_type = random.choice(["viewed", "submitted", "graded", "posted"])
                mdl_logstore_standard_log.append(
                    {
                        "id": log_id,
                        "eventname": f"\\core\\event\\{event_type}",
                        "component": "core",
                        "action": event_type,
                        "target": "course",
                        "objectid": cid,
                        "userid": uid,
                        "courseid": cid,
                        "contextid": cid,
                        "timecreated": int(ts.timestamp()),
                        "origin": "web",
                    }
                )
                event_log.append(
                    {
                        "user_id": uid,
                        "course_id": cid,
                        "timestamp": ts,
                        "event_type": event_type,
                    }
                )
                log_id += 1


# ---------------- Course Completion (core-like) ---------------- #
# Create 1 simple completion criterion per course (assign module)
for cid, _ in COURSES:
    assign_list = course_assignments.get(cid, [])
    moduleinstance = assign_list[0] if assign_list else None
    mdl_course_completion_criteria.append(
        {
            "id": ccc_id,
            "course": cid,
            "criteriatype": 4,  # module completion (proxy)
            "module": "assign",
            "moduleinstance": moduleinstance if moduleinstance is not None else 0,
            "courseinstance": 0,
            "enrolperiod": None,
            "timeend": None,
            "gradepass": 60.0,
            "role": 0,
        }
    )
    ccc_id += 1

# Completion records per user/course (simple: completed if submitted all assignments)
for cid, _ in COURSES:
    course_students = [
        e["user_id"] for e in enrol_fact if e["course_id"] == cid and e["user_id"] in student_ids
    ]
    assign_list = set(course_assignments.get(cid, []))
    for sid in course_students:
        user_subs = [
            s for s in submission_fact if s["course_id"] == cid and s["user_id"] == sid
        ]
        submitted_acts = {s["activity_id"] for s in user_subs if s["submitted_at"] is not None}
        completed = assign_list.issubset(submitted_acts) if assign_list else False

        time_enrolled = next(
            (e["enrol_time"] for e in enrol_fact if e["course_id"] == cid and e["user_id"] == sid),
            START_DATE,
        )
        time_started = time_enrolled
        time_completed = None
        if completed:
            last_sub = max(s["submitted_at"] for s in user_subs if s["submitted_at"] is not None)
            time_completed = last_sub

        mdl_course_completions.append(
            {
                "id": cc_id,
                "userid": sid,
                "course": cid,
                "timeenrolled": int(time_enrolled.timestamp()),
                "timestarted": int(time_started.timestamp()),
                "timecompleted": int(time_completed.timestamp()) if time_completed else None,
                "reaggregate": 0,
            }
        )
        cc_id += 1

        # completion criteria per user
        for crit in mdl_course_completion_criteria:
            if crit["course"] != cid:
                continue
            gradefinal = None
            if completed:
                grades = [
                    g["score"]
                    for g in grade_fact
                    if g["course_id"] == cid and g["user_id"] == sid
                ]
                gradefinal = round(sum(grades) / len(grades), 2) if grades else None
            mdl_course_completion_crit_compl.append(
                {
                    "id": cccc_id,
                    "userid": sid,
                    "course": cid,
                    "criteriaid": crit["id"],
                    "gradefinal": gradefinal,
                    "unenroled": None,
                    "timecompleted": int(time_completed.timestamp()) if time_completed else None,
                }
            )
            cccc_id += 1

# ---------------- Forum (Pending questions) ---------------- #
mdl_forum: List[Dict] = []
mdl_forum_discussions: List[Dict] = []
mdl_forum_posts: List[Dict] = []
forum_posts_slim: List[Dict] = []

forum_id = 1
discussion_id = 1
post_id = 1

for cid, _ in COURSES:
    mdl_forum.append(
        {
            "id": forum_id,
            "course": cid,
            "name": f"Forum {cid}",
            "type": "general",
            "timecreated": int(START_DATE.timestamp()),
        }
    )
    # create discussions with some unanswered
    for _ in range(12):
        author = random.choice(student_ids)
        t = START_DATE + timedelta(days=random.randint(0, 120))
        mdl_forum_discussions.append(
            {
                "id": discussion_id,
                "course": cid,
                "forum": forum_id,
                "name": f"Discussion {discussion_id}",
                "userid": author,
                "timecreated": int(t.timestamp()),
            }
        )
        # main post
        main_post_id = post_id
        mdl_forum_posts.append(
            {
                "id": post_id,
                "discussion": discussion_id,
                "userid": author,
                "parent": 0,
                "created": int(t.timestamp()),
                "subject": f"Question {post_id}",
            }
        )
        forum_posts_slim.append(
            {
                "course_id": cid,
                "discussion_id": discussion_id,
                "post_id": post_id,
                "user_id": author,
                "parent_id": 0,
                "created_at": t,
            }
        )
        post_id += 1
        # replies (50% unanswered)
        if random.random() > 0.5:
            reply_count = random.randint(1, 3)
            for _ in range(reply_count):
                replier = random.choice(teacher_ids + student_ids)
                rt = t + timedelta(hours=random.randint(1, 72))
                mdl_forum_posts.append(
                    {
                        "id": post_id,
                        "discussion": discussion_id,
                        "userid": replier,
                        "parent": main_post_id,
                        "created": int(rt.timestamp()),
                        "subject": f"Re: {post_id}",
                    }
                )
                forum_posts_slim.append(
                    {
                        "course_id": cid,
                        "discussion_id": discussion_id,
                        "post_id": post_id,
                        "user_id": replier,
                        "parent_id": main_post_id,
                        "created_at": rt,
                    }
                )
                post_id += 1
        discussion_id += 1
    forum_id += 1


# ---------------- Feedback (Course rating) ---------------- #
mdl_feedback: List[Dict] = []
mdl_feedback_item: List[Dict] = []
mdl_feedback_completed: List[Dict] = []
mdl_feedback_value: List[Dict] = []
course_rating: List[Dict] = []

feedback_id = 1
item_id = 1
completed_id = 1
value_id = 1

for cid, _ in COURSES:
    mdl_feedback.append(
        {
            "id": feedback_id,
            "course": cid,
            "name": f"Course rating {cid}",
            "timeopen": int(START_DATE.timestamp()),
            "timeclose": int(END_DATE.timestamp()),
        }
    )
    mdl_feedback_item.append(
        {
            "id": item_id,
            "feedback": feedback_id,
            "name": "Overall rating",
            "typ": "numeric",
        }
    )
    ratings = []
    for sid in random.sample(student_ids, k=min(len(student_ids), 20)):
        rt = START_DATE + timedelta(days=random.randint(10, 120))
        score = round(random.uniform(3.0, 5.0), 2)
        mdl_feedback_completed.append(
            {
                "id": completed_id,
                "feedback": feedback_id,
                "userid": sid,
                "timemodified": int(rt.timestamp()),
            }
        )
        mdl_feedback_value.append(
            {
                "id": value_id,
                "item": item_id,
                "completed": completed_id,
                "value": score,
            }
        )
        ratings.append(score)
        completed_id += 1
        value_id += 1
    course_rating.append(
        {
            "course_id": cid,
            "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
            "num_ratings": len(ratings),
        }
    )
    feedback_id += 1
    item_id += 1


# ---------------- Daily KPIs ---------------- #
daily_course_kpi: List[Dict] = []
for cid, _ in COURSES:
    events_by_day = {}
    for ev in event_log:
        if ev["course_id"] != cid:
            continue
        day = ev["timestamp"].date()
        events_by_day.setdefault(day, set()).add(ev["user_id"])
    for day in daterange(START_DATE, END_DATE):
        day_date = day.date()
        active_users = len(events_by_day.get(day_date, set()))
        subs = [
            s
            for s in submission_fact
            if s["course_id"] == cid
            and s["submitted_at"] is not None
            and s["submitted_at"].date() == day_date
        ]
        submissions = len(subs)
        completions = sum(
            1 for s in subs if s["submitted_at"] is not None and s["submitted_at"] <= s["duedate"]
        )
        grades_today = [
            g["score"]
            for g in grade_fact
            if g["course_id"] == cid and g["graded_at"].date() == day_date
        ]
        avg_grade = round(sum(grades_today) / len(grades_today), 2) if grades_today else 0
        daily_course_kpi.append(
            {
                "course_id": cid,
                "date": day_date.isoformat(),
                "active_users": active_users,
                "submissions": submissions,
                "completions": completions,
                "avg_grade": avg_grade,
            }
        )


# ---------------- Write CSV helper ---------------- #
def write_csv(name: str, fieldnames: List[str], rows: List[Dict]):
    path = BASE / name
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# Simplified (for API/UI)
write_csv("user_dim.csv", ["user_id", "fullname", "role", "created_at"], user_dim)
write_csv("course_dim.csv", ["course_id", "fullname", "category", "startdate"], course_dim)
write_csv("enrol_fact.csv", ["course_id", "user_id", "enrol_time"], enrol_fact)
write_csv(
    "grade_fact.csv",
    ["course_id", "user_id", "item_id", "score", "maxscore", "graded_at"],
    grade_fact,
)
write_csv(
    "submission_fact.csv",
    ["course_id", "user_id", "activity_id", "submitted_at", "duedate"],
    submission_fact,
)
write_csv(
    "event_log_staging.csv",
    ["user_id", "course_id", "timestamp", "event_type"],
    event_log,
)
write_csv(
    "daily_course_kpi.csv",
    ["course_id", "date", "active_users", "submissions", "completions", "avg_grade"],
    daily_course_kpi,
)
write_csv(
    "forum_posts.csv",
    ["course_id", "discussion_id", "post_id", "user_id", "parent_id", "created_at"],
    forum_posts_slim,
)

# Pending questions (derived: discussions where main post has no replies)
pending = {}
for row in forum_posts_slim:
    if row["parent_id"] == 0:
        pending[row["discussion_id"]] = {
            "course_id": row["course_id"],
            "question_post_id": row["post_id"],
            "has_reply": False,
        }
    else:
        if row["discussion_id"] in pending:
            pending[row["discussion_id"]]["has_reply"] = True
pending_rows = [
    {"course_id": v["course_id"], "question_post_id": v["question_post_id"]}
    for v in pending.values()
    if not v["has_reply"]
]
write_csv("pending_questions.csv", ["course_id", "question_post_id"], pending_rows)

write_csv("course_rating.csv", ["course_id", "avg_rating", "num_ratings"], course_rating)

# ---------------- Mentor datasets ---------------- #
idea_dim: List[Dict] = []
mentor_profile: List[Dict] = []
mentor_match: List[Dict] = []
mentor_meeting: List[Dict] = []
action_item: List[Dict] = []
pitch_readiness: List[Dict] = []

domain_codes = ["edtech", "ai", "health", "fintech", "sustainability"]
stages = ["idea", "prototype", "mvp", "growth"]

idea_id = 1
idea_pool = enrolled_student_ids if enrolled_student_ids else student_ids
for sid in random.sample(idea_pool, k=min(len(idea_pool), 30)):
    idea_dim.append(
        {
            "idea_id": idea_id,
            "student_id": sid,
            "domain_code": random.choice(domain_codes),
            "title": f"Idea {idea_id}",
            "stage": random.choice(stages),
            "created_at": START_DATE.date(),
        }
    )
    idea_id += 1

mentor_profile_ids = random.sample(mentor_ids, k=min(len(mentor_ids), 3))
for mid in mentor_profile_ids:
    mentor_profile.append(
        {
            "mentor_id": mid,
            "primary_domain_code": random.choice(domain_codes),
            "years_exp": random.randint(2, 15),
        }
    )

match_id = 1
meeting_id = 1
action_id = 1
for idea in idea_dim:
    if random.random() < 0.7:
        mentor_id = random.choice(mentor_profile_ids)
        if random.random() < 0.25:
            matched_at = END_DATE - timedelta(days=random.randint(0, 6))
        else:
            matched_at = START_DATE + timedelta(days=random.randint(10, 120))
        mentor_match.append(
            {
                "match_id": match_id,
                "idea_id": idea["idea_id"],
                "mentor_id": mentor_id,
                "matched_at": matched_at,
                "status": random.choice(["active", "completed"]),
            }
        )
        # meetings
        for _ in range(random.randint(1, 3)):
            mt = matched_at + timedelta(days=random.randint(1, 30))
            mentor_meeting.append(
                {
                    "meeting_id": meeting_id,
                    "match_id": match_id,
                    "meeting_time": mt,
                    "mode": random.choice(["online", "offline"]),
                    "notes": "Session notes",
                }
            )
            meeting_id += 1
        # action items
        for _ in range(random.randint(1, 4)):
            due = matched_at + timedelta(days=random.randint(7, 45))
            done = due + timedelta(days=random.randint(0, 7)) if random.random() > 0.35 else None
            action_item.append(
                {
                    "action_id": action_id,
                    "match_id": match_id,
                    "title": f"Action {action_id}",
                    "owner": random.choice(["mentor", "student"]),
                    "due_date": due,
                    "status": "done" if done else "open",
                    "done_at": done,
                }
            )
            action_id += 1
        base_score = max(45, min(97, random.gauss(75, 12)))
        if matched_at >= END_DATE - timedelta(days=14):
            base_score = min(99, base_score + random.randint(3, 10))
        pitch_readiness.append(
            {
                "match_id": match_id,
                "score_0_100": int(round(base_score)),
                "rated_at": matched_at + timedelta(days=random.randint(5, 60)),
            }
        )
        match_id += 1

write_csv(
    "idea_dim.csv",
    ["idea_id", "student_id", "domain_code", "title", "stage", "created_at"],
    idea_dim,
)
write_csv(
    "mentor_profile.csv",
    ["mentor_id", "primary_domain_code", "years_exp"],
    mentor_profile,
)
write_csv(
    "mentor_match.csv",
    ["match_id", "idea_id", "mentor_id", "matched_at", "status"],
    mentor_match,
)
write_csv(
    "mentor_meeting.csv",
    ["meeting_id", "match_id", "meeting_time", "mode", "notes"],
    mentor_meeting,
)
write_csv(
    "action_item.csv",
    ["action_id", "match_id", "title", "owner", "due_date", "status", "done_at"],
    action_item,
)
write_csv(
    "pitch_readiness.csv",
    ["match_id", "score_0_100", "rated_at"],
    pitch_readiness,
)

# Core-like tables
write_csv(
    "mdl_user.csv",
    ["id", "auth", "confirmed", "deleted", "suspended", "mnethostid", "username", "firstname", "lastname", "email", "timecreated"],
    mdl_user,
)
write_csv("mdl_course_categories.csv", ["id", "name", "timecreated"], mdl_course_categories)
write_csv(
    "mdl_course.csv",
    ["id", "category", "fullname", "shortname", "startdate", "timecreated"],
    mdl_course,
)
write_csv("mdl_enrol.csv", ["id", "enrol", "status", "courseid", "timecreated"], mdl_enrol)
write_csv(
    "mdl_user_enrolments.csv",
    ["id", "enrolid", "userid", "timestart"],
    mdl_user_enrolments,
)
write_csv(
    "mdl_assign.csv",
    ["id", "course", "name", "duedate", "timemodified"],
    mdl_assign,
)
write_csv(
    "mdl_assign_submission.csv",
    ["id", "assignment", "userid", "status", "timecreated", "timemodified"],
    mdl_assign_submission,
)
write_csv(
    "mdl_grade_items.csv",
    ["id", "courseid", "itemtype", "itemmodule", "iteminstance", "itemname", "grademax"],
    mdl_grade_items,
)
write_csv(
    "mdl_grade_grades.csv",
    ["id", "itemid", "userid", "rawgrade", "finalgrade", "timemodified"],
    mdl_grade_grades,
)
write_csv(
    "mdl_course_modules.csv",
    ["id", "course", "module", "instance", "section"],
    mdl_course_modules,
)
write_csv(
    "mdl_course_modules_completion.csv",
    ["id", "coursemoduleid", "userid", "completionstate", "timemodified"],
    mdl_course_modules_completion,
)
write_csv(
    "mdl_course_modules_viewed.csv",
    ["id", "coursemoduleid", "userid", "timecreated"],
    mdl_course_modules_viewed,
)
write_csv(
    "mdl_logstore_standard_log.csv",
    ["id", "eventname", "component", "action", "target", "objectid", "userid", "courseid", "contextid", "timecreated", "origin"],
    mdl_logstore_standard_log,
)
write_csv(
    "mdl_course_completion_criteria.csv",
    ["id", "course", "criteriatype", "module", "moduleinstance", "courseinstance", "enrolperiod", "timeend", "gradepass", "role"],
    mdl_course_completion_criteria,
)
write_csv(
    "mdl_course_completion_crit_compl.csv",
    ["id", "userid", "course", "criteriaid", "gradefinal", "unenroled", "timecompleted"],
    mdl_course_completion_crit_compl,
)
write_csv(
    "mdl_course_completions.csv",
    ["id", "userid", "course", "timeenrolled", "timestarted", "timecompleted", "reaggregate"],
    mdl_course_completions,
)
write_csv(
    "mdl_forum.csv",
    ["id", "course", "name", "type", "timecreated"],
    mdl_forum,
)
write_csv(
    "mdl_forum_discussions.csv",
    ["id", "course", "forum", "name", "userid", "timecreated"],
    mdl_forum_discussions,
)
write_csv(
    "mdl_forum_posts.csv",
    ["id", "discussion", "userid", "parent", "created", "subject"],
    mdl_forum_posts,
)
write_csv(
    "mdl_feedback.csv",
    ["id", "course", "name", "timeopen", "timeclose"],
    mdl_feedback,
)
write_csv(
    "mdl_feedback_item.csv",
    ["id", "feedback", "name", "typ"],
    mdl_feedback_item,
)
write_csv(
    "mdl_feedback_completed.csv",
    ["id", "feedback", "userid", "timemodified"],
    mdl_feedback_completed,
)
write_csv(
    "mdl_feedback_value.csv",
    ["id", "item", "completed", "value"],
    mdl_feedback_value,
)

# ---------------- Extra lightweight CSVs ---------------- #
session_fact: List[Dict] = []
for u in user_dim:
    for _ in range(random.randint(2, 6)):
        login_at = START_DATE + timedelta(days=random.randint(0, 120), hours=random.randint(6, 22))
        last_seen_at = login_at + timedelta(minutes=random.randint(5, 180))
        logout_at = last_seen_at + timedelta(minutes=random.randint(1, 30))
        session_fact.append(
            {
                "user_id": u["user_id"],
                "login_at": login_at,
                "last_seen_at": last_seen_at,
                "logout_at": logout_at,
            }
        )

error_log: List[Dict] = []
services = ["api", "db", "auth", "web"]
for _ in range(80):
    ts = START_DATE + timedelta(days=random.randint(0, 150), hours=random.randint(0, 23))
    error_log.append(
        {
            "timestamp": ts,
            "service": random.choice(services),
            "error_code": random.choice(["E500", "E502", "E401", "E403", "E_TIMEOUT"]),
            "severity": random.choice(["low", "medium", "high"]),
        }
    )

db_metrics_daily: List[Dict] = []
db_size = 512
for day in daterange(START_DATE, END_DATE):
    db_size += random.uniform(0.2, 1.5)
    db_metrics_daily.append(
        {
            "date": day.date(),
            "db_size_mb": round(db_size, 2),
        }
    )

user_status: List[Dict] = []
for u in random.sample(user_dim, k=min(len(user_dim), 12)):
    blocked_at = START_DATE + timedelta(days=random.randint(5, 140))
    user_status.append(
        {
            "user_id": u["user_id"],
            "status": random.choice(["active", "blocked", "suspended"]),
            "reason": random.choice(["spam", "policy", "security", "inactive"]),
            "blocked_at": blocked_at,
        }
    )

mentor_availability: List[Dict] = []
for mid in mentor_profile_ids:
    for _ in range(random.randint(3, 6)):
        start_time = START_DATE + timedelta(days=random.randint(0, 120), hours=random.randint(8, 18))
        end_time = start_time + timedelta(hours=random.randint(1, 3))
        mentor_availability.append(
            {
                "mentor_id": mid,
                "start_time": start_time,
                "end_time": end_time,
                "is_free": random.choice([0, 1]),
            }
        )
    # ensure some availability near "today"
    for _ in range(2):
        start_time = END_DATE + timedelta(hours=random.randint(8, 18))
        end_time = start_time + timedelta(hours=2)
        mentor_availability.append(
            {
                "mentor_id": mid,
                "start_time": start_time,
                "end_time": end_time,
                "is_free": 1,
            }
        )

write_csv(
    "session_fact.csv",
    ["user_id", "login_at", "last_seen_at", "logout_at"],
    session_fact,
)
write_csv(
    "error_log.csv",
    ["timestamp", "service", "error_code", "severity"],
    error_log,
)
write_csv(
    "db_metrics_daily.csv",
    ["date", "db_size_mb"],
    db_metrics_daily,
)
write_csv(
    "user_status.csv",
    ["user_id", "status", "reason", "blocked_at"],
    user_status,
)
write_csv(
    "mentor_availability.csv",
    ["mentor_id", "start_time", "end_time", "is_free"],
    mentor_availability,
)

print(f"Wrote CSVs to {BASE} (simplified + core-like)")
