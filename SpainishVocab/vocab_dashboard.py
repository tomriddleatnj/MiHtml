from flask import Flask, render_template_string, jsonify, request, send_from_directory
import sqlite3
import json
import os

app = Flask(__name__)
DB_NAME = "vocab_project.db"
EXPORT_DIR = "exported_slots"

if not os.path.exists(EXPORT_DIR):
    os.makedirs(EXPORT_DIR)

AVAILABLE_MODELS = ["gemini-2.5-flash", "gemini-2.0-flash-exp", "gemini-1.5-flash", "gemini-1.5-pro"]

def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def home():
    try:
        with open('vocab_dashboard.html', 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return "Error: vocab_dashboard.html not found."

# ==================== 配置 API ====================
@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    conn = get_db()
    if request.method == 'POST':
        new_model = request.json.get('model_name')
        if new_model:
            conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES ('model_name', ?)", (new_model,))
            conn.commit()
            conn.close()
            return jsonify({"success": True, "model": new_model})
    
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT value FROM app_config WHERE key='model_name'")
        row = cursor.fetchone()
        current_model = row['value'] if row else "gemini-2.5-flash"
    except:
        current_model = "gemini-2.5-flash"
    conn.close()
    return jsonify({"current_model": current_model, "available_models": AVAILABLE_MODELS})

# ★★★ 新增：Worker 状态控制 API ★★★
@app.route('/api/worker_status', methods=['GET', 'POST'])
def api_worker_status():
    conn = get_db()
    if request.method == 'POST':
        # 设置状态 ('running' 或 'paused')
        new_status = request.json.get('status')
        if new_status in ['running', 'paused']:
            conn.execute("INSERT OR REPLACE INTO app_config (key, value) VALUES ('worker_status', ?)", (new_status,))
            conn.commit()
            conn.close()
            return jsonify({"success": True, "status": new_status})
        conn.close()
        return jsonify({"error": "Invalid status"}), 400

    # GET 状态
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT value FROM app_config WHERE key='worker_status'")
        row = cursor.fetchone()
        # 默认为 paused，安全第一
        status = row['value'] if row else "paused"
    except:
        status = "paused"
    conn.close()
    return jsonify({"status": status})

# ==================== 监控 API (含翻译统计) ====================
@app.route('/api/stats')
def api_stats():
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # 基础统计
        cursor.execute("SELECT count(*) FROM vocab_staging")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT count(*) FROM vocab_staging WHERE processed_flag = 1")
        processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT count(*) FROM vocab_staging WHERE processed_flag = 2")
        errors = cursor.fetchone()[0]
        
        cursor.execute("SELECT count(*) FROM vocab_staging WHERE status='keep' AND processed_flag=1")
        kept = cursor.fetchone()[0]
        
        cursor.execute("SELECT count(*) FROM vocab_staging WHERE status='discard' AND processed_flag=1")
        discarded = cursor.fetchone()[0]
        
        try:
            cursor.execute("SELECT count(*) FROM vocab_staging WHERE status='keep' AND translated_flag=1")
            translated = cursor.fetchone()[0]
        except:
            translated = 0
            
        # ★★★ 修改这里：将 LIMIT 5 改为 LIMIT 50，让日志窗口显示更多内容 ★★★
        cursor.execute("SELECT word, tags, status, level, updated_at, processed_flag FROM vocab_staging WHERE processed_flag IN (1, 2) ORDER BY updated_at DESC LIMIT 50")
        recent_logs = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            "total": total, 
            "processed": processed, 
            "errors": errors,
            "kept": kept,
            "discarded": discarded,
            "translated": translated,
            "percent_classify": round((processed + errors)/total*100, 1) if total else 0,
            "percent_translate": round(translated/kept*100, 1) if kept else 0,
            "recent_logs": recent_logs
        })
    except Exception as e:
        return jsonify({"error": str(e)})

# ==================== 触发/重置 API ====================
@app.route('/api/trigger_translate', methods=['POST'])
def trigger_translate():
    try:
        conn = get_db()
        conn.execute("UPDATE vocab_staging SET translated_flag = 0 WHERE status = 'keep'")
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "Translation queue reset."})
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/api/reset_discards', methods=['POST'])
def reset_discards():
    try:
        conn = get_db()
        conn.execute("UPDATE vocab_staging SET status='pending', processed_flag=0, tags='[]' WHERE status='discard'")
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "Discarded words reset to Pending."})
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/api/retry_errors', methods=['POST'])
def retry_errors():
    try:
        conn = get_db()
        conn.execute("UPDATE vocab_staging SET processed_flag=0 WHERE processed_flag=2")
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "Error items queued for retry."})
    except Exception as e: return jsonify({"error": str(e)})

# ==================== 导出与槽位 ====================
@app.route('/api/slots')
def api_slots():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT slot_id, filename, count(word) as count FROM word_slots GROUP BY slot_id")
    slots_data = {row['slot_id']: dict(row) for row in cursor.fetchall()}
    conn.close()
    return jsonify(slots_data)

def build_filter_query(filter_type, filter_values):
    query = "SELECT word, level, hint, tags, definition_cn, phonetic, context FROM vocab_staging WHERE status = 'keep'"
    params = []
    if not filter_values: return query, params

    if filter_type == 'tag':
        sub_conditions = []
        for val in filter_values:
            sub_conditions.append('tags LIKE ?')
            params.append(f'%"{val}"%')
        if sub_conditions: query += f" AND ({' OR '.join(sub_conditions)})"
            
    elif filter_type == 'level':
        placeholders = ','.join(['?'] * len(filter_values))
        query += f" AND level IN ({placeholders})"
        params.extend(filter_values)
        
    return query, params

@app.route('/api/preview_export', methods=['POST'])
def preview_export():
    data = request.json
    conn = get_db()
    query, params = build_filter_query(data.get('filter_type'), data.get('filter_values', []))
    cursor = conn.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return jsonify({"count": len(rows)})

@app.route('/api/do_export', methods=['POST'])
def do_export():
    data = request.json
    slot_id = int(data.get('slot_id'))
    filename = f"{slot_id:02d}.json"
    filepath = os.path.join(EXPORT_DIR, filename)
    conn = get_db()
    
    query, params = build_filter_query(data.get('filter_type'), data.get('filter_values', []))
    rows = conn.execute(query, params).fetchall()
    
    export_data = []
    for r in rows:
        cn_def = r['definition_cn'] if r['definition_cn'] else r['hint']
        item = {
            "es_term": r['word'],
            "en_term": r['hint'],
            "definition": cn_def,
            "phonetic": r['phonetic'] or "",
            "context": r['context'] or "",
            "level": r['level'],
            "tags": json.loads(r['tags'])
        }
        export_data.append(item)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("[\n")
            total = len(export_data)
            for i, item in enumerate(export_data):
                line = json.dumps(item, ensure_ascii=False, separators=(', ', ': '))
                comma = "," if i < total - 1 else ""
                f.write(f"  {line}{comma}\n")
            f.write("]")
    except Exception as e: return jsonify({"error": str(e)})
        
    conn.execute("DELETE FROM word_slots WHERE slot_id = ?", (slot_id,))
    if export_data:
        conn.executemany("INSERT INTO word_slots (slot_id, word, filename) VALUES (?, ?, ?)", 
                        [(slot_id, r['word'], filename) for r in rows])
    conn.commit()
    conn.close()
        
    return jsonify({"success": True, "message": f"Exported {len(export_data)} words", "filename": filename})

@app.route('/api/clear_slot', methods=['POST'])
def clear_slot():
    try:
        data = request.json
        slot_id = int(data.get('slot_id'))
        filename = f"{slot_id:02d}.json"
        filepath = os.path.join(EXPORT_DIR, filename)
        if os.path.exists(filepath): os.remove(filepath)
        conn = get_db()
        conn.execute("DELETE FROM word_slots WHERE slot_id = ?", (slot_id,))
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "Slot cleared."})
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    return send_from_directory(EXPORT_DIR, filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True, port=5000)