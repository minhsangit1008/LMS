from fastapi import FastAPI

from .student import router as student_router
from .teacher_course import router as teacher_course_router
from .teacher_overall import router as teacher_overall_router

app = FastAPI(title="LMS API", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(student_router)
app.include_router(teacher_course_router)
app.include_router(teacher_overall_router)
