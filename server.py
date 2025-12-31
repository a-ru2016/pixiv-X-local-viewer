import os
import json
import sqlite3
import re
import uvicorn
import urllib.parse
import threading
import concurrent.futures
import hashlib
from contextlib import asynccontextmanager
from collections import defaultdict
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- 設定 ---
CONFIG_FILE = "universal_config.json"
DB_NAME = "universal_data.db"

current_config = {"target_dir": ""}
is_scanning = False
scan_status_msg = ""
scan_lock = threading.Lock()

# --- DB初期化 ---
def init_db():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=-64000;") 
    
    c = conn.cursor()
    
    # 1. Pixiv Tables
    c.execute('''CREATE TABLE IF NOT EXISTS pixiv_works (
        id TEXT PRIMARY KEY,
        title TEXT,
        author TEXT,
        timestamp INTEGER,
        total_pages INTEGER,
        is_liked INTEGER DEFAULT 0,
        folder_name TEXT,
        file_mtime INTEGER
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS pixiv_pages (
        work_id TEXT,
        page_num INTEGER,
        file_path TEXT,
        media_type TEXT,
        FOREIGN KEY(work_id) REFERENCES pixiv_works(id)
    )''')

    # 2. Tweet Tables
    c.execute('''CREATE TABLE IF NOT EXISTS tweets (
        id TEXT PRIMARY KEY,
        timestamp INTEGER,
        user_name TEXT,
        text TEXT,
        json_path TEXT,
        tweet_url TEXT,
        avatar_url TEXT,
        folder_name TEXT,
        file_mtime INTEGER,
        is_liked INTEGER DEFAULT 0
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS tweet_media (
        tweet_id TEXT,
        file_path TEXT,
        media_type TEXT,
        FOREIGN KEY(tweet_id) REFERENCES tweets(id)
    )''')
    
    # 3. Others Tables
    c.execute('''CREATE TABLE IF NOT EXISTS other_works (
        id TEXT PRIMARY KEY,
        filename TEXT,
        folder_name TEXT,
        file_path TEXT,
        media_type TEXT,
        file_mtime INTEGER,
        is_liked INTEGER DEFAULT 0
    )''')

    # Indexes
    for table in ['pixiv_works', 'tweets', 'other_works']:
        c.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {table} (file_mtime)')
        c.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_folder ON {table} (folder_name)')
        try: c.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_liked ON {table} (is_liked)')
        except: pass

    c.execute('CREATE INDEX IF NOT EXISTS idx_pixiv_pages_wid ON pixiv_pages (work_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tweet_media_tid ON tweet_media (tweet_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_other_path ON other_works (file_path)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_pixiv_path ON pixiv_pages (file_path)')
    
    conn.commit()
    return conn

# --- スキャンロジック ---
def scan_worker(args):
    root, files, existing_pixiv, existing_tweets, existing_others = args
    
    pixiv_buffer = {} 
    tweet_buffer = []
    other_buffer = []
    
    folder_name = os.path.basename(root)
    
    pixiv_folder_match = re.search(r'[\-_](\d{3,})$', folder_name)
    is_pixiv_folder = bool(pixiv_folder_match)
    pixiv_author_id = pixiv_folder_match.group(1) if is_pixiv_folder else None
    
    pixiv_file_pattern = re.compile(r'^(\d{5,})(?:_p(\d+))?')
    tweet_id_pattern = re.compile(r'_(\d{14,})(_|\.)')

    media_files = []
    json_files = []
    
    files = [f for f in files if not f.startswith('.')]
    for f in files:
        low = f.lower()
        if low.endswith('.json'): json_files.append(f)
        elif low.endswith(('.jpg', '.jpeg', '.png', '.gif', '.mp4', '.webm')): media_files.append(f)

    used_media = set()

    # === 1. Twitter (JSON優先) ===
    media_map_for_tweets = defaultdict(list)
    for f in media_files:
        tm = tweet_id_pattern.search(f)
        if tm: media_map_for_tweets[tm.group(1)].append(f)

    for jf in json_files:
        nums = re.findall(r'(\d{14,})', jf)
        if not nums: continue
        tid = nums[0]

        if tid in existing_tweets: 
            for m in media_map_for_tweets.get(tid, []): used_media.add(m)
            continue

        try:
            full_path = os.path.join(root, jf)
            try:
                with open(full_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except: continue

            mtime = int(os.path.getmtime(full_path) * 1000)
            
            raw_ts = data.get('timestamp') or data.get('saved_at') or 0
            try: ts = int(raw_ts)
            except: ts = 0

            user = data.get('user_name') or data.get('user') or 'Unknown'
            text = data.get('text') or ''
            t_url = data.get('url') or f"https://twitter.com/{user}/status/{tid}"
            av_url = data.get('avatar_url') or data.get('user_icon') or ""
            
            t_media = []
            for mf in media_map_for_tweets.get(tid, []):
                used_media.add(mf)
                m_path = os.path.join(root, mf)
                m_type = 'video' if mf.lower().endswith(('.mp4', '.webm')) else 'image'
                t_media.append((tid, m_path, m_type))
            
            tweet_buffer.append(((tid, ts, user, text, full_path, t_url, av_url, folder_name, mtime, 0), t_media))
        except: pass

    # === 2. Pixiv ===
    if is_pixiv_folder:
        author_match = re.match(r'^(.*?)[\s\-_]*(\d+)$', folder_name)
        author_name = author_match.group(1).strip() if (author_match and author_match.group(1).strip()) else folder_name

        for mf in media_files:
            if mf in used_media: continue

            pm = pixiv_file_pattern.search(mf)
            
            if pm:
                wid = pm.group(1)
                pg_str = pm.group(2)
                p_num = int(pg_str) if pg_str else 0
            else:
                wid = f"folder_{pixiv_author_id}_{hashlib.md5(mf.encode()).hexdigest()[:8]}"
                p_num = 0

            if wid in existing_pixiv: 
                used_media.add(mf)
                continue

            full_path = os.path.join(root, mf)
            mtime = int(os.path.getmtime(full_path) * 1000)
            m_type = 'video' if mf.lower().endswith(('.mp4', '.webm')) else 'image'

            if wid not in pixiv_buffer:
                pixiv_buffer[wid] = {
                    "title": mf, "author": author_name, "timestamp": mtime, 
                    "folder": folder_name, "pages": []
                }
            
            if mtime > pixiv_buffer[wid]["timestamp"]:
                pixiv_buffer[wid]["timestamp"] = mtime
            
            pixiv_buffer[wid]["pages"].append((p_num, full_path, m_type))
            used_media.add(mf)
    
    # === 3. Others ===
    for mf in media_files:
        if mf in used_media: continue
        
        full_path = os.path.join(root, mf)
        file_id = hashlib.md5(full_path.encode()).hexdigest()
        
        if file_id in existing_others: continue
        
        mtime = int(os.path.getmtime(full_path) * 1000)
        m_type = 'video' if mf.lower().endswith(('.mp4', '.webm')) else 'image'
        
        other_buffer.append((file_id, mf, folder_name, full_path, m_type, mtime, 0))

    if tweet_buffer or pixiv_buffer or other_buffer:
        pass

    return (tweet_buffer, pixiv_buffer, other_buffer)

def run_scan(target_dir: str):
    global is_scanning, scan_status_msg
    with scan_lock:
        if is_scanning: return
        is_scanning = True
    
    print(f"=== SCAN START: {target_dir} ===")
    
    try:
        conn = init_db()
        c = conn.cursor()
        c.execute("SELECT id FROM pixiv_works")
        ex_pixiv = set(r[0] for r in c.fetchall())
        c.execute("SELECT id FROM tweets")
        ex_tweets = set(r[0] for r in c.fetchall())
        c.execute("SELECT id FROM other_works")
        ex_others = set(r[0] for r in c.fetchall())
        
        tasks = []
        for root, dirs, files in os.walk(target_dir):
            if not files: continue
            parts = root.split(os.sep)
            if any(p.startswith('.') and len(p) > 1 for p in parts): continue
            tasks.append((root, files, ex_pixiv, ex_tweets, ex_others))
            
        scan_status_msg = f"{len(tasks)} フォルダ..."
        
        new_cnt = {"t":0, "p":0, "o":0}
        max_workers = min(32, (os.cpu_count() or 4) * 2)
        
        b_pix_w, b_pix_p = [], []
        b_tweets, b_tweet_m = [], []
        b_others = []
        BATCH = 200
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(scan_worker, t) for t in tasks]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    t_buf, p_buf, o_buf = future.result()
                    
                    for t_data, m_list in t_buf:
                        b_tweets.append(t_data)
                        b_tweet_m.extend(m_list)
                        new_cnt["t"] += 1
                    
                    for wid, data in p_buf.items():
                        data["pages"].sort(key=lambda x: x[0])
                        final_pages = []
                        for i, (_, path, mtype) in enumerate(data["pages"]):
                            final_pages.append((wid, i, path, mtype))
                        b_pix_w.append((wid, data["title"], data["author"], data["timestamp"], len(final_pages), 0, data["folder"], data["timestamp"]))
                        b_pix_p.extend(final_pages)
                        new_cnt["p"] += 1
                        
                    b_others.extend(o_buf)
                    new_cnt["o"] += len(o_buf)
                    
                    if len(b_tweets) >= BATCH:
                        c.executemany("INSERT OR IGNORE INTO tweets VALUES (?,?,?,?,?,?,?,?,?,?)", b_tweets)
                        c.executemany("INSERT OR IGNORE INTO tweet_media VALUES (?,?,?)", b_tweet_m)
                        conn.commit(); b_tweets, b_tweet_m = [], []
                        
                    if len(b_pix_w) >= BATCH:
                        c.executemany("INSERT OR IGNORE INTO pixiv_works VALUES (?,?,?,?,?,?,?,?)", b_pix_w)
                        c.executemany("INSERT OR IGNORE INTO pixiv_pages VALUES (?,?,?,?)", b_pix_p)
                        conn.commit(); b_pix_w, b_pix_p = [], []
                        
                    if len(b_others) >= BATCH:
                        c.executemany("INSERT OR IGNORE INTO other_works VALUES (?,?,?,?,?,?,?)", b_others)
                        conn.commit(); b_others = []
                    
                    if sum(new_cnt.values()) % 100 == 0:
                        scan_status_msg = f"Pixiv:{new_cnt['p']} Tweet:{new_cnt['t']} Other:{new_cnt['o']}"
                        
                except Exception as e:
                    print(f"Task Error: {e}")

        if b_tweets:
            c.executemany("INSERT OR IGNORE INTO tweets VALUES (?,?,?,?,?,?,?,?,?,?)", b_tweets)
            c.executemany("INSERT OR IGNORE INTO tweet_media VALUES (?,?,?)", b_tweet_m)
        if b_pix_w:
            c.executemany("INSERT OR IGNORE INTO pixiv_works VALUES (?,?,?,?,?,?,?,?)", b_pix_w)
            c.executemany("INSERT OR IGNORE INTO pixiv_pages VALUES (?,?,?,?)", b_pix_p)
        if b_others:
            c.executemany("INSERT OR IGNORE INTO other_works VALUES (?,?,?,?,?,?,?)", b_others)
        
        conn.commit()
        
        print("Cleaning up duplicates...")
        c.execute("""
            DELETE FROM other_works 
            WHERE file_path IN (SELECT file_path FROM pixiv_pages) 
               OR file_path IN (SELECT file_path FROM tweet_media)
        """)
        deleted_count = c.rowcount
        conn.commit()
        conn.close()
        
        scan_status_msg = f"完了: P{new_cnt['p']} / T{new_cnt['t']} / O{new_cnt['o']} (Del:{deleted_count})"
        print(f"=== COMPLETE: {scan_status_msg} ===")
        
    except Exception as e:
        scan_status_msg = f"エラー: {e}"
        import traceback; traceback.print_exc()
    finally:
        is_scanning = False

# --- FastAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global current_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: current_config = json.load(f)
        except: pass
    init_db()
    target = current_config.get("target_dir")
    if target and os.path.exists(target):
        app.mount("/files", StaticFiles(directory=target), name="files")
        threading.Thread(target=run_scan, args=(target,), daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/viewer", StaticFiles(directory=".", html=True), name="static")

class PathRequest(BaseModel): path: str
class LikeRequest(BaseModel): liked: bool

@app.get("/api/status")
def get_status():
    return {"is_scanning": is_scanning, "message": scan_status_msg, "target_dir": current_config.get("target_dir", "")}

@app.post("/api/settings/path")
def set_path(req: PathRequest):
    if not os.path.exists(req.path): raise HTTPException(400, "Path not found")
    global current_config
    current_config["target_dir"] = req.path
    with open(CONFIG_FILE, 'w') as f: json.dump(current_config, f)
    for r in app.routes:
        if r.path == "/files": app.routes.remove(r); break
    app.mount("/files", StaticFiles(directory=req.path), name="files")
    if not is_scanning: threading.Thread(target=run_scan, args=(req.path,), daemon=True).start()
    return {"status": "ok"}

@app.get("/api/folders")
def get_folders(source: str = "all"):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    folders = set()
    targets = []
    if source == "all": targets = ['pixiv_works', 'tweets', 'other_works']
    elif source == "pixiv": targets = ['pixiv_works']
    elif source == "tweets": targets = ['tweets']
    elif source == "others": targets = ['other_works']
    
    for t in targets:
        try:
            c.execute(f"SELECT DISTINCT folder_name FROM {t}")
            folders.update(r[0] for r in c.fetchall())
        except: pass
    conn.close()
    return sorted(list(folders))

@app.post("/api/like/{type}/{id}")
def toggle_like(type: str, id: str, req: LikeRequest):
    conn = sqlite3.connect(DB_NAME)
    val = 1 if req.liked else 0
    table = "pixiv_works" if type == "pixiv" else "tweets" if type == "tweet" else "other_works"
    conn.execute(f"UPDATE {table} SET is_liked = ? WHERE id = ?", (val, id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

# --- Stream API with Direction ---
@app.get("/api/stream")
def get_stream(
    limit: int = 50, 
    offset: int = 0, 
    source: str = "all", 
    q: str = None, 
    sort: str = "desc", 
    folder: str = None, 
    filter_type: str = "all", 
    target_date: int = None,
    direction: str = "older" # older, newer, around
):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    base_dir = current_config.get("target_dir", "")
    
    def to_url(path):
        try: return "/files/" + urllib.parse.quote(os.path.relpath(path, base_dir).replace("\\", "/"))
        except: return ""

    def safe_int(v):
        try: return int(v)
        except: return 0

    order_col = "file_mtime"
    
    # 共通クエリビルダ
    def fetch_ids(fetch_dir, fetch_limit, fetch_target=None):
        # fetch_dir: 'older' (<=, DESC) or 'newer' (>, ASC)
        
        op = "<=" if fetch_dir == "older" else ">"
        order_s = "DESC" if fetch_dir == "older" else "ASC"
        
        # UIのsort指定が'asc'（古い順）の場合、意味が反転するが、
        # ここでは標準的な「タイムライン基準」で実装（older=過去へ, newer=未来へ）
        # もしUI全体がASCソートならクライアント側でdirectionを逆にする等の制御も可能だが
        # 今回は「新しい順(DESC)」を基本ビューとして実装する
        
        # ユーザー指定のsortがascの場合は、older/newerの概念が逆転する
        # シンプル化のため、内部では常に file_mtime で処理し、
        # direction='older' は 数値が小さい方へ、 'newer' は大きい方へとする
        
        # 正確には:
        # sort='desc' (default): Top is Newest. Scroll Down -> Older. Scroll Up -> Newer.
        # direction='older' -> Get items <= target (Down)
        # direction='newer' -> Get items > target (Up)
        
        # sort='asc' (Oldest first): Top is Oldest. Scroll Down -> Newer.
        # この場合クライアントが適切にパラメータを送る想定
        
        if sort == "asc":
            # ASCモード: 下に行くと新しい
            if fetch_dir == "older": # Next page (Down) -> Newer items
                op = ">="; order_s = "ASC"
            else: # Prev page (Up) -> Older items
                op = "<"; order_s = "DESC"
        else:
            # DESCモード (Default): 下に行くと古い
            if fetch_dir == "older": # Next page (Down) -> Older items
                op = "<="; order_s = "DESC"
            else: # Prev page (Up) -> Newer items
                op = ">"; order_s = "ASC"

        results = []
        
        def build_query(table, type_label):
            cols = f"id, {order_col} as sort_key"
            where = []
            params = []
            
            if folder and folder != "ALL":
                where.append("folder_name = ?")
                params.append(folder)
            if q:
                if table == "pixiv_works": where.append("(title LIKE ? OR author LIKE ?)"); params.extend([f"%{q}%"]*2)
                elif table == "tweets": where.append("(text LIKE ? OR user_name LIKE ?)"); params.extend([f"%{q}%"]*2)
                elif table == "other_works": where.append("(filename LIKE ?)"); params.append(f"%{q}%")

            if filter_type == "liked": where.append("is_liked = 1")
            elif filter_type == "text":
                if table != "tweets": where.append("1=0")
                else: where.append("NOT EXISTS (SELECT 1 FROM tweet_media WHERE tweet_id = tweets.id)")
            elif filter_type == "video":
                if table == "pixiv_works": where.append("EXISTS (SELECT 1 FROM pixiv_pages p WHERE p.work_id = pixiv_works.id AND p.media_type = 'video')")
                elif table == "tweets": where.append("EXISTS (SELECT 1 FROM tweet_media m WHERE m.tweet_id = tweets.id AND m.media_type = 'video')")
                elif table == "other_works": where.append("media_type = 'video'")
            elif filter_type == "image":
                if table == "pixiv_works": where.append("NOT EXISTS (SELECT 1 FROM pixiv_pages p WHERE p.work_id = pixiv_works.id AND p.media_type = 'video')")
                elif table == "tweets": where.append("EXISTS (SELECT 1 FROM tweet_media m WHERE m.tweet_id = tweets.id) AND NOT EXISTS (SELECT 1 FROM tweet_media m WHERE m.tweet_id = tweets.id AND m.media_type = 'video')")
                elif table == "other_works": where.append("media_type = 'image'")

            if fetch_target is not None:
                where.append(f"{order_col} {op} ?")
                params.append(fetch_target)

            clause = " WHERE " + " AND ".join(where) if where else ""
            return f"SELECT '{type_label}' as type, {cols} FROM {table} {clause}", params

        # Fetch from Tables
        tables = []
        if source in ["all", "pixiv"]: tables.append(("pixiv_works", "pixiv"))
        if source in ["all", "tweets"]: tables.append(("tweets", "tweet"))
        if source in ["all", "others"]: tables.append(("other_works", "other"))

        for t_name, t_type in tables:
            q_str, p = build_query(t_name, t_type)
            c.execute(f"SELECT id, sort_key FROM ({q_str}) ORDER BY sort_key {order_s} LIMIT ?", p + [fetch_limit + offset])
            results.extend([(t_type, r[0], safe_int(r[1])) for r in c.fetchall()])
        
        # Sort Combined
        reverse = True if order_s == "DESC" else False
        results.sort(key=lambda x: x[2], reverse=reverse)
        
        # Pagination Slice (Merged List)
        return results[offset : offset + fetch_limit]

    # --- Main Logic ---
    final_ids = []
    
    if direction == "around" and target_date is not None:
        # Before & After
        half = limit // 2
        newer_list = fetch_ids("newer", half, target_date) # Future
        older_list = fetch_ids("older", half, target_date) # Past (includes target)
        
        # Newer list comes in ASC (closest to target first), needs to be reversed to stack on top
        if sort == "desc": newer_list.reverse()
        # If ASC mode, Older list (Prev page) needs reverse
        else: older_list.reverse()
        
        final_ids = newer_list + older_list
        
    elif direction == "newer":
        # Load Previous (Up)
        final_ids = fetch_ids("newer", limit, target_date)
        # If DESC sort, 'newer' fetches ASC (old->new). We want to show them above, so reverse.
        if sort == "desc": final_ids.reverse()
        
    else: # older (Standard Down)
        final_ids = fetch_ids("older", limit, target_date)

    # --- Hydrate Details ---
    final_data = []
    for r_type, r_id, _ in final_ids:
        if r_type == 'pixiv':
            c.execute("SELECT * FROM pixiv_works WHERE id = ?", (r_id,))
            row = c.fetchone()
            if not row: continue
            c.execute("SELECT page_num, file_path, media_type FROM pixiv_pages WHERE work_id = ? ORDER BY page_num", (r_id,))
            pages = [{"url": to_url(p[1]), "type": p[2], "index": p[0]} for p in c.fetchall()]
            final_data.append({
                "type": "pixiv", "id": row[0], "title": row[1], "author": row[2], "timestamp": row[7],
                "is_liked": bool(row[5]), "folder": row[6], "media": pages, "pixiv_url": f"https://www.pixiv.net/artworks/{row[0]}"
            })
        elif r_type == 'tweet':
            c.execute("SELECT * FROM tweets WHERE id = ?", (r_id,))
            row = c.fetchone()
            if not row: continue
            c.execute("SELECT file_path, media_type FROM tweet_media WHERE tweet_id = ?", (r_id,))
            media = [{"url": to_url(m[0]), "type": m[1]} for m in c.fetchall()]
            final_data.append({
                "type": "tweet", "id": row[0], "timestamp": row[8],
                "user_name": row[2], "text": row[3], "tweet_url": row[5], "avatar_url": row[6], "folder": row[7], "is_liked": bool(row[9]), "media": media
            })
        elif r_type == 'other':
            c.execute("SELECT * FROM other_works WHERE id = ?", (r_id,))
            row = c.fetchone()
            if not row: continue
            media = [{"url": to_url(row[3]), "type": row[4]}]
            final_data.append({
                "type": "other", "id": row[0], "timestamp": row[5],
                "title": row[1], "folder": row[2], "is_liked": bool(row[6]), "media": media
            })
            
    conn.close()
    return final_data

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)