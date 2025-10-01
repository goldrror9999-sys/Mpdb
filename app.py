
import os
import sqlite3
import secrets
import re
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import pymysql

# ---------------------------
# CONFIG - change via env vars
# ---------------------------
# MySQL admin credentials (used to CREATE DATABASE for projects)
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_ADMIN_USER = os.environ.get("MYSQL_ADMIN_USER", "root")
MYSQL_ADMIN_PASSWORD = os.environ.get("MYSQL_ADMIN_PASSWORD", "")

# App config
APP_SECRET = os.environ.get("MPDB_SECRET", "dev_secret_change_me")
PORT = int(os.environ.get("MPDB_PORT", "5000"))

# ---------------------------
# Ensure folders
# ---------------------------
os.makedirs("database", exist_ok=True)

# app metadata stored locally (lightweight)
META_DB = os.path.join("database", "mpdb_meta.sqlite")

# ---------------------------
# Flask app
# ---------------------------
app = Flask(__name__)
app.secret_key = APP_SECRET

# Fixed owner credentials (change in env or code)
OWNER_USERNAME = os.environ.get("MPDB_OWNER_USER", "owner")
OWNER_PASSWORD = os.environ.get("MPDB_OWNER_PASS", "1234")

# ---------------------------
# Meta DB helpers (SQLite) - to store projects and keys
# ---------------------------
def meta_conn():
    conn = sqlite3.connect(META_DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_meta():
    c = meta_conn()
    c.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        password TEXT,
        privacy TEXT,
        mysql_db TEXT,
        api_key TEXT
    )
    """)
    c.commit()
    c.close()

init_meta()

# ---------------------------
# MySQL admin helpers
# ---------------------------
def mysql_admin_conn():
    return pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT,
                           user=MYSQL_ADMIN_USER, password=MYSQL_ADMIN_PASSWORD,
                           cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def create_mysql_database(dbname):
    """Create database/schema on MySQL server."""
    conn = mysql_admin_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    finally:
        conn.close()

def run_sql_on_project_db(dbname, sql_script):
    """Run SQL script on given MySQL database (owner only). Returns (success, message or rows)."""
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT,
                           user=MYSQL_ADMIN_USER, password=MYSQL_ADMIN_PASSWORD,
                           database=dbname,
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)
    try:
        with conn.cursor() as cur:
            # use executescript-like for pymysql: split by ';' (naive) and execute non-empty
            statements = [s.strip() for s in sql_script.split(';') if s.strip()]
            results = []
            for s in statements:
                cur.execute(s)
                # If SELECT, fetch rows
                if s.strip().lower().startswith("select"):
                    rows = cur.fetchall()
                    results.append({"statement": s, "rows": rows})
            return True, results
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

# ---------------------------
# Utilities
# ---------------------------
SELECT_RE = re.compile(r'^\s*select\b', re.IGNORECASE)

def is_select_only(sql_text):
    # rough check: allow only statements that start with SELECT (skip whitespace & comments)
    # For safety also reject semicolons inside strings etc (basic)
    stmts = [s.strip() for s in sql_text.split(';') if s.strip()]
    if not stmts:
        return False
    for s in stmts:
        if not SELECT_RE.match(s):
            return False
    return True

# ---------------------------
# Routes
# ---------------------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username","").strip()
        p = request.form.get("password","").strip()
        if u == OWNER_USERNAME and p == OWNER_PASSWORD:
            session['owner'] = True
            flash("Login successful", "success")
            return redirect(url_for("dashboard"))
        flash("Invalid credentials", "danger")
        return redirect(url_for("login"))
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.pop("owner", None)
    flash("Logged out", "info")
    return redirect(url_for("login"))

@app.route("/dashboard")
def dashboard():
    if 'owner' not in session:
        return redirect(url_for("login"))
    c = meta_conn()
    rows = c.execute("SELECT * FROM projects ORDER BY id DESC").fetchall()
    projects = [dict(r) for r in rows]
    c.close()
    return render_template("dashboard.html", projects=projects)

@app.route("/create_project", methods=["POST"])
def create_project():
    if 'owner' not in session:
        return redirect(url_for("login"))
    name = request.form.get("name","").strip()
    password = request.form.get("password","").strip()
    privacy = request.form.get("privacy","Private").strip()
    if not name or not password:
        flash("Name & password required", "danger")
        return redirect(url_for("dashboard"))
    # sanitize name for DB name (keep alphanum and underscore)
    safe_name = re.sub(r'[^A-Za-z0-9_]', '_', name)
    mysql_db = f"mpdb_proj_{safe_name}"
    # create mysql database
    try:
        create_mysql_database(mysql_db)
    except Exception as e:
        flash(f"MySQL error creating DB: {e}", "danger")
        return redirect(url_for("dashboard"))
    # insert meta
    c = meta_conn()
    try:
        c.execute("INSERT INTO projects (name,password,privacy,mysql_db) VALUES (?,?,?,?)",
                  (name, password, privacy, mysql_db))
        c.commit()
    except Exception as e:
        flash(f"Error saving project meta: {e}", "danger")
    finally:
        c.close()
    flash(f"Project '{name}' created (MySQL DB: {mysql_db})", "success")
    return redirect(url_for("dashboard"))

@app.route("/project/<int:pid>")
def project_view(pid):
    if 'owner' not in session:
        return redirect(url_for("login"))
    c = meta_conn()
    row = c.execute("SELECT * FROM projects WHERE id=?",(pid,)).fetchone()
    c.close()
    if not row:
        flash("Project not found", "danger")
        return redirect(url_for("dashboard"))
    proj = dict(row)
    # list tables in mysql db
    try:
        conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT,
                               user=MYSQL_ADMIN_USER, password=MYSQL_ADMIN_PASSWORD,
                               database=proj['mysql_db'],
                               cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES;")
            tables = [ list(t.values())[0] for t in cur.fetchall() ]
        conn.close()
    except Exception as e:
        tables = []
        flash(f"Could not list tables: {e}", "warning")
    return render_template("project.html", project=proj, tables=tables)

@app.route("/project/<int:pid>/execute", methods=["POST"])
def project_execute(pid):
    if 'owner' not in session:
        return redirect(url_for("login"))
    sql = request.form.get("sql","").strip()
    if not sql:
        flash("No SQL provided", "danger")
        return redirect(url_for("project_view", pid=pid))
    c = meta_conn()
    row = c.execute("SELECT * FROM projects WHERE id=?",(pid,)).fetchone()
    c.close()
    if not row:
        flash("Project not found", "danger")
        return redirect(url_for("dashboard"))
    proj = dict(row)
    success, result = run_sql_on_project_db(proj['mysql_db'], sql)
    if not success:
        flash(f"SQL error: {result}", "danger")
        return redirect(url_for("project_view", pid=pid))
    # If there are SELECT results show them on a result page
    return render_template("sql_result.html", project=proj, results=result, sql=sql)

@app.route("/project/<int:pid>/generate_jumbo", methods=["POST"])
def generate_jumbo(pid):
    if 'owner' not in session:
        return redirect(url_for("login"))
    c = meta_conn()
    row = c.execute("SELECT * FROM projects WHERE id=?",(pid,)).fetchone()
    if not row:
        c.close()
        flash("Project not found", "danger")
        return redirect(url_for("dashboard"))
    key = secrets.token_hex(28)
    c.execute("UPDATE projects SET api_key=? WHERE id=?",(key, pid))
    c.commit()
    c.close()
    flash("Jumbo API key generated. Save it securely.", "success")
    return redirect(url_for("project_view", pid=pid))

# public read endpoint (read-only)
@app.route("/api/public/<project_name>/query", methods=["POST"])
def public_query(project_name):
    data = request.get_json(force=True) or {}
    api_key = data.get("api_key")
    sql = data.get("sql","").strip()
    if not api_key or not sql:
        return jsonify({"error":"api_key and sql (SELECT) required"}), 400
    # lookup project
    c = meta_conn()
    row = c.execute("SELECT * FROM projects WHERE name=? AND api_key=? AND privacy='Publish'", (project_name, api_key)).fetchone()
    c.close()
    if not row:
        return jsonify({"error":"Invalid key or project not published"}), 403
    proj = dict(row)
    # only allow SELECT statements (basic check)
    if not is_select_only(sql):
        return jsonify({"error":"Only SELECT statements allowed on public API"}), 400
    # limit rows forcibly: add LIMIT if not present (naive)
    if "limit" not in sql.lower():
        sql = sql.rstrip(';') + " LIMIT 500;"
    try:
        conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT,
                               user=MYSQL_ADMIN_USER, password=MYSQL_ADMIN_PASSWORD,
                               database=proj['mysql_db'],
                               cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        conn.close()
        return jsonify({"columns": list(rows[0].keys()) if rows else [], "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Simple table view (owner)
@app.route("/project/<int:pid>/table/<table_name>")
def project_table_view(pid, table_name):
    if 'owner' not in session:
        return redirect(url_for("login"))
    c = meta_conn()
    row = c.execute("SELECT * FROM projects WHERE id=?",(pid,)).fetchone()
    c.close()
    if not row:
        flash("Project not found", "danger")
        return redirect(url_for("dashboard"))
    proj = dict(row)
    try:
        conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT,
                               user=MYSQL_ADMIN_USER, password=MYSQL_ADMIN_PASSWORD,
                               database=proj['mysql_db'],
                               cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{table_name}` LIMIT 500;")
            rows = cur.fetchall()
            columns = list(rows[0].keys()) if rows else []
        conn.close()
    except Exception as e:
        flash(f"Error reading table: {e}", "danger")
        return redirect(url_for("project_view", pid=pid))
    return render_template("table_view.html", project=proj, table_name=table_name, columns=columns, rows=rows)

# ---------------------------
# Error / Run
# ---------------------------
@app.errorhandler(404)
def not_found(e):
    return "Not Found", 404

if __name__ == "__main__":
    # Ensure meta DB file exists and writable
    open(META_DB, "a").close()
    try:
        os.chmod(META_DB, 0o664)
    except Exception:
        pass
    # run
    app.run(host="0.0.0.0", port=PORT, debug=True)