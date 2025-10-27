# "List" was added here
from typing import Optional, List 
from fastapi import APIRouter, Form, HTTPException, Header, Request
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_302_FOUND
from jose import jwt, JWTError
from datetime import datetime, timedelta

from . import database
import os

# This is new
from .app_schemas import UserListItem

router = APIRouter()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "../templates/html"))

@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("Login.html", {"request": request, "error": None})

# This is modified
@router.post("/login")
async def login_user(
    request: Request,
    user_id: Optional[str] = Form(None),
    role: Optional[str] = Form("employee"),
    email: Optional[str] = Form(None),
    password: str = Form(...),
    accept: Optional[str] = Header(default="application/json")
):
    conn = database.get_db_connection()
    try:
        user = None
        # If user_id provided (from dropdown), use role-aware lookup
        if user_id:
            user = database.get_user_by_id_from_table(conn, role=role, _id=user_id)
        else:
            # fallback to email-based auth (search both tables via your existing helper)
            user = database.get_user_by_email(email)

        if not user:
            if "text/html" in accept:
                return templates.TemplateResponse("Login.html", {"request": request, "error": "Invalid credentials"})
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # verify password (assumes get_user_by_id_from_table returns 'password_hash' or compatible)
        if not database.verify_password(user.get("password_hash") or user.get("password"), password):
            if "text/html" in accept:
                return templates.TemplateResponse("Login.html", {"request": request, "error": "Invalid credentials"})
            raise HTTPException(status_code=401, detail="Invalid credentials")

        # login success: set session or produce token (keep your existing flow)
        if "text/html" in accept:
            request.session["user"] = {"id": user["id"], "role": role}
            return RedirectResponse(url="/", status_code=HTTP_302_FOUND)
        else:
            token = create_access_token({"id": user["id"], "role": role})
            return {"access_token": token, "token_type": "bearer"}
    finally:
        try:
            conn.close()
        except Exception:
            pass
# Modification ends here

#MOBILE APP
SECRET_KEY = "your-secret-key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 1 day

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


@router.get("/logout")
def logout_user(request: Request):
    request.session.clear()
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session", path="/")  
    return response


def get_current_user(request: Request):
    user = request.session.get("user")
    if not user or "id" not in user:
        return None
    return user

# This one is new:
@router.get("/api/users/list", response_model=List[UserListItem])
async def api_users_list(role: Optional[str] = "employee", q: Optional[str] = None, limit: int = 50):
    conn = database.get_db_connection()
    try:
        rows = database.list_active_users_by_role(conn, role=role, q=q, limit=limit)
        return [{"id": r["id"], "display_name": r["display_name"]} for r in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass