import sqlite3
import google.generativeai as genai
import json
import time
import random
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 配置区域 =================
# ★★★ Paid Tier API Key Config ★★★
API_KEY = "AIzaSyDZgWK8dJr13C9SdFtAq1Hm_9YOgI1edZI"

# Paid Tier Optimization Config
BATCH_SIZE = 50           # 每个 API 请求包含的单词数
MAX_WORKERS = 20           # 并发线程数
SUPER_BATCH_SIZE = BATCH_SIZE * MAX_WORKERS 

DB_NAME = "vocab_project.db"
SOURCE_FILE = "wordsdata_es.txt"
DEFAULT_MODEL = "gemini-2.5-flash" 

TAG_LIST_STR = """
[Professional Tags]
office, hr, finance, legal, it, ops, marketing, bd, procurement, qhse,
pm, bidding, supervision, water, transport, rail, roads, airport, ports, energy, urban, geo, environment

[General Categories]
comm (communication), abstract (logic/time/numbers), society (politics/history)
"""

if API_KEY == "gen-lang-client-0577078086":
    print("⚠️ 警告: 请在 vocab_worker.py 中配置正确的 API_KEY")
else:
    genai.configure(api_key=API_KEY)

# ================= 辅助函数 =================

def clean_json_string(text):
    text = re.sub(r'^```json\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'^```\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'\s*```$', '', text, flags=re.MULTILINE)
    return text.strip()

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute('PRAGMA journal_mode=WAL;') 
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS vocab_staging (
            word TEXT PRIMARY KEY,
            level TEXT,
            hint TEXT,
            tags TEXT,
            status TEXT,
            processed_flag INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    existing_cols = [row[1] for row in cursor.execute("PRAGMA table_info(vocab_staging)")]
    new_columns = {
        "definition_cn": "TEXT",
        "phonetic": "TEXT",
        "context": "TEXT",
        "translated_flag": "INTEGER DEFAULT 0" 
    }
    for col_name, col_type in new_columns.items():
        if col_name not in existing_cols:
            print(f"🔧 升级数据库: 添加 {col_name}...")
            cursor.execute(f"ALTER TABLE vocab_staging ADD COLUMN {col_name} {col_type}")

    cursor.execute('''CREATE TABLE IF NOT EXISTS word_slots (slot_id INTEGER, word TEXT, filename TEXT, exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (slot_id, word))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, value TEXT)''')
    
    # 默认配置
    cursor.execute("INSERT OR IGNORE INTO app_config (key, value) VALUES (?, ?)", ('model_name', DEFAULT_MODEL))
    # ★★★ 新增: 默认状态为 paused，防止启动即暴走 ★★★
    cursor.execute("INSERT OR IGNORE INTO app_config (key, value) VALUES (?, ?)", ('worker_status', 'paused'))
    
    conn.commit()
    return conn

def get_config_value(conn, key, default=None):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM app_config WHERE key=?", (key,))
        row = cursor.fetchone()
        return row[0] if row else default
    except:
        return default

def load_data_to_db(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM vocab_staging")
    if cursor.fetchone()[0] > 0: return

    if not os.path.exists(SOURCE_FILE):
        print(f"❌ 错误: 找不到源文件 {SOURCE_FILE}")
        return

    print("📥 正在导入原始数据...")
    with open(SOURCE_FILE, 'r', encoding='utf-8') as f:
        to_insert = []
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 3: continue
            word, level, hint = parts[0].strip(), parts[-2].strip(), parts[-1].strip()
            to_insert.append((word, level, hint, "[]", "pending", 0))
    
    cursor.executemany('INSERT OR IGNORE INTO vocab_staging (word, level, hint, tags, status, processed_flag) VALUES (?, ?, ?, ?, ?, ?)', to_insert)
    conn.commit()
    print(f"✅ 成功导入 {len(to_insert)} 条数据。")

# ================= AI 任务逻辑 (并发版) =================

def call_ai_with_retry(model, prompt, model_name, task_type):
    retries = 3
    base_wait = 1
    
    for attempt in range(retries):
        try:
            response = model.generate_content(prompt)
            cleaned = clean_json_string(response.text)
            return json.loads(cleaned)
        except Exception as e:
            err_msg = str(e)
            print(f"  ⚠️ {task_type} Error ({model_name} - {attempt+1}/{retries}): {err_msg}")
            
            if "429" in err_msg:
                wait_match = re.search(r'retry in (\d+(\.\d+)?)s', err_msg)
                wait_time = float(wait_match.group(1)) + 1 if wait_match else 5
                print(f"  🛑 Rate Limit (429). Thread sleeping {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            
            time.sleep(base_wait)
            
    return None 

def process_classify_chunk(chunk_data, model_name):
    model = genai.GenerativeModel(model_name, generation_config={"response_mime_type": "application/json"})
    input_list = [f"{w} (Def: {h})" for w, l, h in chunk_data]
    
    prompt = f"""
    Role: Spanish linguistic expert.
    Tags: {TAG_LIST_STR}
    Task: Classify words. Return empty tags [] if no fit.
    Input: {json.dumps(input_list)}
    Output JSON: [{{"word": "word1", "tags": ["tag1"]}}]
    """
    
    result = call_ai_with_retry(model, prompt, model_name, "Classify")
    return chunk_data, result

def process_translate_chunk(chunk_data, model_name):
    model = genai.GenerativeModel(model_name, generation_config={"response_mime_type": "application/json"})
    input_list = [{"word": w, "hint": h} for w, _, h in chunk_data]
    
    prompt = f"""
    Role: Expert Spanish-Chinese Translator.
    Task: Provide Chinese definition, IPA phonetic, and a simple Spanish context sentence for each word.
    Input: {json.dumps(input_list)}
    Output JSON Format:
    [
      {{"word": "ordenador", "definition": "电脑", "phonetic": "/oɾ.ðe.naˈðoɾ/", "context": "Mi ordenador es nuevo."}}
    ]
    """
    
    result = call_ai_with_retry(model, prompt, model_name, "Translate")
    return chunk_data, result

# ================= 主程序 =================

def main():
    conn = init_db()
    load_data_to_db(conn)
    cursor = conn.cursor()
    
    print(f"🚀 Worker 启动 (Parallel Mode) | Threads: {MAX_WORKERS}")
    
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    
    # 状态打印去重
    last_status_print = ""

    while True:
        # ★★★ 控制逻辑核心 ★★★
        # 1. 获取最新配置
        current_model = get_config_value(conn, 'model_name', DEFAULT_MODEL)
        worker_status = get_config_value(conn, 'worker_status', 'paused')

        # 2. 如果暂停，则空转
        if worker_status != 'running':
            if last_status_print != "paused":
                print(f"⏸️ Worker 已暂停 (Status: {worker_status}). 等待指令...")
                last_status_print = "paused"
            time.sleep(2)
            continue
        
        if last_status_print != "running":
            print(f"▶️ Worker 运行中 (Model: {current_model})...")
            last_status_print = "running"

        # --- 3. Classify Logic ---
        cursor.execute("SELECT word, level, hint FROM vocab_staging WHERE processed_flag = 0 LIMIT ?", (SUPER_BATCH_SIZE,))
        super_batch = cursor.fetchall()
        
        if super_batch:
            print(f"\n🔄 [Classify] Processing {len(super_batch)} words...")
            chunks = [super_batch[i:i + BATCH_SIZE] for i in range(0, len(super_batch), BATCH_SIZE)]
            
            futures = []
            for chunk in chunks:
                futures.append(executor.submit(process_classify_chunk, chunk, current_model))
            
            db_updates = []
            db_errors = []
            
            for future in as_completed(futures):
                original_chunk, res_json = future.result()
                if res_json is None:
                    for w, l, h in original_chunk: db_errors.append((w,))
                else:
                    res_map = {i['word']: i.get('tags', []) for i in res_json if 'word' in i}
                    for w, l, h in original_chunk:
                        tags = res_map.get(w, [])
                        status = 'keep' if tags or l in ['A1', 'A2', 'B1'] else 'discard'
                        final_tags = tags if tags else (['basic'] if status=='keep' else [])
                        db_updates.append((json.dumps(final_tags), status, 1, w))

            if db_updates:
                cursor.executemany("UPDATE vocab_staging SET tags=?, status=?, processed_flag=?, updated_at=CURRENT_TIMESTAMP WHERE word=?", db_updates)
                print(f"  ✅ Saved {len(db_updates)} classifications.")
            if db_errors:
                cursor.executemany("UPDATE vocab_staging SET processed_flag=2, updated_at=CURRENT_TIMESTAMP WHERE word=?", db_errors)
            
            conn.commit()
            continue 

        # --- 4. Translate Logic ---
        cursor.execute("SELECT word, level, hint FROM vocab_staging WHERE status='keep' AND translated_flag=0 LIMIT ?", (SUPER_BATCH_SIZE,))
        super_batch_trans = cursor.fetchall()
        
        if super_batch_trans:
            print(f"\n🔤 [Translate] Processing {len(super_batch_trans)} words...")
            chunks = [super_batch_trans[i:i + BATCH_SIZE] for i in range(0, len(super_batch_trans), BATCH_SIZE)]
            
            futures = []
            for chunk in chunks:
                futures.append(executor.submit(process_translate_chunk, chunk, current_model))
            
            db_updates = []
            db_errors = []
            
            for future in as_completed(futures):
                original_chunk, res_json = future.result()
                if res_json is None:
                    for w, l, h in original_chunk: db_errors.append((w,))
                else:
                    res_map = {i['word']: i for i in res_json if 'word' in i}
                    for w, l, h in original_chunk:
                        info = res_map.get(w, {})
                        db_updates.append((info.get('definition', ''), info.get('phonetic', ''), info.get('context', ''), 1, w))

            if db_updates:
                cursor.executemany("UPDATE vocab_staging SET definition_cn=?, phonetic=?, context=?, translated_flag=?, updated_at=CURRENT_TIMESTAMP WHERE word=?", db_updates)
                print(f"  ✅ Saved {len(db_updates)} translations.")
            if db_errors:
                cursor.executemany("UPDATE vocab_staging SET translated_flag=2, updated_at=CURRENT_TIMESTAMP WHERE word=?", db_errors)
                
            conn.commit()
            continue

        print("💤 Queue Empty. Waiting 5s...")
        time.sleep(5)

if __name__ == "__main__":
    main()