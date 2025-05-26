from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.db.connection_manager import ConnectionManager

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
conn_mgr = ConnectionManager()

@router.get("/connections", response_class=HTMLResponse)
def get_connections(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "connections": conn_mgr.list_connections(),
        "selected": conn_mgr.active_connection,
    })

@router.post("/connections/add", response_class=HTMLResponse)
def add_connection(request: Request, name: str = Form(...), server: str = Form(...), db_name: str = Form(...)):
    try:
        conn_mgr.add_connection(name, server, db_name)
    except ValueError as e:
        return HTMLResponse(f"<p>Error: {str(e)}</p>", status_code=400)
    return get_connections(request)

@router.post("/connections/delete", response_class=HTMLResponse)
def delete_connection(request: Request, name: str = Form(...)):
    try:
        conn_mgr.delete_connection(name)
    except ValueError as e:
        return HTMLResponse(f"<p>Error: {str(e)}</p>", status_code=400)
    return get_connections(request)

@router.post("/connections/select", response_class=HTMLResponse)
def select_connection(request: Request, name: str = Form(...)):
    try:
        conn_mgr.select_connection(name)
    except ValueError as e:
        return HTMLResponse(f"<p>Error: {str(e)}</p>", status_code=400)
    return get_connections(request)

@router.post("/query", response_class=HTMLResponse)
def run_query(request: Request, sql: str = Form(...)):
    try:
        engine = conn_mgr.get_selected_engine()
        with engine.connect() as conn:
            result = conn.execute(sql)
            rows = [dict(row) for row in result]
    except Exception as e:
        return HTMLResponse(f"<p>Error: {str(e)}</p>", status_code=500)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "connections": conn_mgr.list_connections(),
        "selected": conn_mgr.active_connection,
        "rows": rows
    })
