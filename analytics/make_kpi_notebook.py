"""
Create KPI/Chart demo notebook from synthetic CSVs in LMS/db/sample_data.
"""
from pathlib import Path
import nbformat as nbf


def build_notebook() -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    cells = []

    cells.append(
        nbf.v4.new_markdown_cell(
            "# KPI & Chart Demo (Synthetic, Moodle-schema aligned)\n\n"
            "Dữ liệu: `LMS/db/sample_data` (faker bám schema core). `course_id` mặc định = 1. "
            "Mỗi khối ghép KPI + Chart đúng mapping vai trò."
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            """from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Locate data dir
candidates = [
    Path(__file__).resolve().parents[2] / "db" / "sample_data",
    Path("LMS/db/sample_data"),
    Path.cwd() / "LMS" / "db" / "sample_data",
]
BASE = next((c for c in candidates if (c/'user_dim.csv').exists()), None)
if BASE is None:
    raise FileNotFoundError('Cannot find LMS/db/sample_data; run gen_demo_for_analytics.py first.')

user_dim = pd.read_csv(BASE/'user_dim.csv')
course_dim = pd.read_csv(BASE/'course_dim.csv')
enrol = pd.read_csv(BASE/'enrol_fact.csv', parse_dates=['enrol_time'])
grade = pd.read_csv(BASE/'grade_fact.csv', parse_dates=['graded_at'])
subm = pd.read_csv(BASE/'submission_fact.csv', parse_dates=['submitted_at','duedate'])
events = pd.read_csv(BASE/'event_log_staging.csv', parse_dates=['timestamp'])
daily = pd.read_csv(BASE/'daily_course_kpi.csv', parse_dates=['date'])

course_id = 1
course_name = course_dim.loc[course_dim.course_id==course_id,'fullname'].iat[0]
print(f\"Using data path: {BASE}\")
print(f\"Course {course_id}: {course_name}\")
print('Daily rows:', len(daily[daily.course_id==course_id]))
course_name"""
        )
    )

    # Student
    cells.append(
        nbf.v4.new_markdown_cell(
            "## Student — KPIs (theo bảng: % hoàn thành, Điểm tôi vs TB vs max, Giờ/phiên hoạt động, Tỉ lệ nộp đúng hạn)\n"
            "- Chart: Completion/grade over time, Grade histogram (điểm), Activity line, Latency bar"
        )
    )
    cells.append(
        nbf.v4.new_code_cell(
            """sel = daily[daily.course_id==course_id].copy().sort_values('date')
fig, axes = plt.subplots(2,2, figsize=(12,8))
axes[0,0].plot(sel['date'], sel['active_users'], label='Active users')
axes[0,0].plot(sel['date'], sel['submissions'], label='Submissions')
axes[0,0].plot(sel['date'], sel['completions'], label='Completions')
axes[0,0].set_title('Completion/Activity over time'); axes[0,0].legend(); axes[0,0].tick_params(axis='x', rotation=45)
axes[0,1].plot(sel['date'], sel['avg_grade'], color='orange'); axes[0,1].set_title('Avg grade over time'); axes[0,1].tick_params(axis='x', rotation=45)

scores = grade[grade.course_id==course_id]['score']
axes[1,0].hist(scores, bins=10, color='steelblue'); axes[1,0].set_title('Grade distribution')

lat = subm[subm.course_id==course_id].copy()
lat['latency_hours'] = (lat['submitted_at']-lat['duedate']).dt.total_seconds()/3600
lat_grp = lat.groupby('activity_id')['latency_hours'].mean()
axes[1,1].bar(lat_grp.index.astype(str), lat_grp.values, color='salmon'); axes[1,1].set_title('Avg submission latency (hrs)'); axes[1,1].set_ylabel('hours')
plt.tight_layout(); plt.show()

subs_total = sel['submissions'].sum()
comp_rate = sel['completions'].sum()/subs_total if subs_total else 0
print(f\"KPI: Completion rate (proxy) = {comp_rate:.2f}\")
print(f\"KPI: Mean grade = {scores.mean():.2f}; Median grade = {scores.median():.2f}\")
events_course = events[events.course_id==course_id]
sessions_per_day = events_course.groupby(events_course['timestamp'].dt.date)['user_id'].nunique().mean()
on_time_rate = lat['latency_hours'].lt(0).mean()
print(f\"KPI: Avg active users per day (sessions proxy) = {sessions_per_day:.2f}\")
print(f\"KPI: On-time submission rate = {on_time_rate:.2%}\")"""
        )
    )

    # Teacher
    cells.append(
        nbf.v4.new_markdown_cell(
            "## Teacher — KPIs (Nộp đúng hạn của lớp, Phân bố điểm bài/quiz, Hoàn thành activity, Học viên nguy cơ)\n"
            "- Chart: On-time vs Late, On-time by activity, Completion per activity, Risk list"
        )
    )
    cells.append(
        nbf.v4.new_code_cell(
            """lat = subm[subm.course_id==course_id].copy()
lat['on_time'] = lat['submitted_at'] <= lat['duedate']
ontime_rate = lat['on_time'].mean()
by_act = lat.groupby('activity_id')['on_time'].mean()
act_order = sorted(lat['activity_id'].unique())
completion_counts = [lat[lat.activity_id==a]['on_time'].sum() for a in act_order]

# Risk (simple): low activity + low grade
recent = events[(events.course_id==course_id)]
activity_counts = recent.groupby('user_id').size()
mean_grade = grade[grade.course_id==course_id].groupby('user_id')['score'].mean()
risk = pd.DataFrame({'events':activity_counts, 'avg_grade':mean_grade}).fillna(0)
risk['risk'] = (risk['events']<risk['events'].median()) & (risk['avg_grade']<risk['avg_grade'].median())
risk_top = risk[risk['risk']].sort_values(['avg_grade','events']).head(10)

fig, axes = plt.subplots(1,3, figsize=(13,4))
axes[0].bar(['On-time','Late'], [lat['on_time'].sum(), (~lat['on_time']).sum()], color=['seagreen','tomato'])
axes[0].set_title('On-time vs Late')
axes[1].bar([str(a) for a in act_order], by_act.values*100, color='cornflowerblue')
axes[1].set_title('On-time rate by activity (%)')
axes[2].plot(range(1,len(act_order)+1), completion_counts, marker='o')
axes[2].set_title('Completion per activity (proxy)'); axes[2].set_xlabel('activity sequence'); axes[2].set_ylabel('count')
plt.tight_layout(); plt.show()

print(f\"KPI: Overall on-time rate = {ontime_rate:.2%}\")
print(\"KPI: Risk learners (low activity & low grade) sample:\\n\", risk_top.head(5))
print(f\"KPI: Avg grade (course) = {mean_grade.mean():.2f}\")
print(f\"KPI: Completion drop-off (min per activity) = {min(completion_counts) if completion_counts else 0}\")"""
        )
    )

    # Investor
    cells.append(
        nbf.v4.new_markdown_cell(
            "## Investor — KPIs (DAU/MAU, Completion/Retention proxy, Trending 7d, Catalog breadth, Enrolments per course)\n"
            "- Chart: DAU line, Trending courses (submissions last 7d), Enrolments per course, Catalog breadth"
        )
    )
    cells.append(
        nbf.v4.new_code_cell(
            """events['date'] = events['timestamp'].dt.date
pau = events.groupby(['date'])['user_id'].nunique()
fig, axes = plt.subplots(2,2, figsize=(12,8))
axes[0,0].plot(pau.index, pau.values)
axes[0,0].set_title('DAU (all courses)'); axes[0,0].tick_params(axis='x', rotation=45)

last7 = subm[subm['submitted_at'] > subm['submitted_at'].max() - pd.Timedelta(days=7)]
trend = last7.groupby('course_id').size().sort_values(ascending=False)
axes[0,1].bar([f\"Course {c}\" for c in trend.index], trend.values, color='mediumpurple')
axes[0,1].set_title('Trending courses (submissions last 7d)'); axes[0,1].tick_params(axis='x', rotation=30)

# Enrolments per course
enrol_counts = enrol.groupby('course_id').size()
axes[1,0].bar([f\"Course {c}\" for c in enrol_counts.index], enrol_counts.values, color='seagreen')
axes[1,0].set_title('Enrolments per course'); axes[1,0].tick_params(axis='x', rotation=30)

# Catalog breadth
course_dim['startdate'] = pd.to_datetime(course_dim['startdate'])
cat_counts = course_dim['category'].value_counts()
axes[1,1].bar(cat_counts.index.astype(str), cat_counts.values, color='darkorange')
axes[1,1].set_title('Catalog breadth (courses by category)')
plt.tight_layout(); plt.show()

mau = events.groupby(events['timestamp'].dt.to_period('M'))['user_id'].nunique()
print('KPI: MAU (per month):', mau.to_dict())
print('KPI: Enrolments per course:', enrol_counts.to_dict())
print('KPI: Catalog breadth (courses by category):', cat_counts.to_dict())"""
        )
    )

    # Admin
    cells.append(
        nbf.v4.new_markdown_cell(
            "## Admin — KPI + Chart (Logs)\n"
            "- KPI: log volume per day, event mix\n"
            "- Chart: line log volume, stacked event types"
        )
    )
    cells.append(
        nbf.v4.new_code_cell(
            """log_daily = events.groupby('date').size()
log_mix = events.groupby(['date','event_type']).size().unstack(fill_value=0)

fig, axes = plt.subplots(1,2, figsize=(12,4))
axes[0].plot(log_daily.index, log_daily.values)
axes[0].set_title('Log volume per day'); axes[0].tick_params(axis='x', rotation=45)
log_mix.plot(ax=axes[1])
axes[1].set_title('Event mix per day'); axes[1].tick_params(axis='x', rotation=45)
plt.tight_layout(); plt.show()

print(f\"KPI: Avg logs per day = {log_daily.mean():.2f}\")
print(\"KPI: Top event types:\\n\", log_mix.sum().sort_values(ascending=False).head())"""
        )
    )

    # Mentor (proxy using completion and events)
    cells.append(
        nbf.v4.new_markdown_cell(
            "## Mentor — KPIs (Tiến độ mentee, Tương tác/tuần)\n"
            "- Chart: Progress per mentee (completion proxy), Interaction per week"
        )
    )
    cells.append(
        nbf.v4.new_code_cell(
            """# Progress per mentee (students) using completion proxy
comp = subm[subm.course_id==course_id].copy()
comp['on_time'] = comp['submitted_at'] <= comp['duedate']
progress = comp.groupby('user_id')['on_time'].mean().sort_values(ascending=False).head(10)

events['week'] = events['timestamp'].dt.to_period('W')
interact = events[events.course_id==course_id].groupby(['user_id','week']).size().groupby('user_id').mean().sort_values(ascending=False).head(10)

fig, axes = plt.subplots(1,2, figsize=(12,4))
axes[0].bar(progress.index.astype(str), progress.values*100, color='teal')
axes[0].set_title('Completion proxy per mentee (top10)'); axes[0].set_ylabel('% on-time')
axes[1].bar(interact.index.astype(str), interact.values, color='slateblue')
axes[1].set_title('Avg interactions/week per mentee (top10)')
plt.tight_layout(); plt.show()

print(f\"KPI: Median completion proxy (mentees) = {progress.median():.2%}\")
print(f\"KPI: Median interactions/week (mentees) = {interact.median():.2f}\")"""
        )
    )

    nb["cells"] = cells
    return nb


def main():
    nb = build_notebook()
    out = Path("LMS/analytics/kpi_chart_demo.ipynb")
    out.write_text(nbf.writes(nb), encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
