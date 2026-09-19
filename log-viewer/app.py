from flask import Flask, request, render_template, jsonify
from datetime import datetime
import os
import re

app = Flask(__name__, static_url_path='', static_folder='static')
LOG_DIR = "logs"  # Folder where log files are stored

@app.route('/')
def root():
    return app.send_static_file('index.html')




@app.route('/search', methods=['POST'])
def search_logs():
    data = request.json
    query = data.get('query', '').strip()
    use_regex = data.get('useRegex', False)
    start_time = data.get('startTime')
    end_time = data.get('endTime')
    logname = data.get('logname')
    logname='NCOMS.log'
    if not logname:
        return jsonify({"error": "No log file specified"}), 400

    log_path = os.path.join(LOG_DIR, logname)
    if not os.path.exists(log_path):
        return jsonify({"error": "Log file not found"}), 404

    try:
        start_dt = datetime.fromisoformat(start_time) if start_time else datetime.min
        end_dt = datetime.fromisoformat(end_time) if end_time else datetime.max
    except ValueError:
        return jsonify({"error": "Invalid time format"}), 400

    filtered_logs = []

    with open(log_path, 'r') as f:
        for line in f:
     
            if not line.strip():
                continue

            # Adjust this regex to your actual log format
            timestamp_match = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}):", line)
            if not timestamp_match:
                continue

            try:
                ts = timestamp_match.group(1).replace(',', '.')
                log_time =datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
       
            except ValueError:
                continue

            if not (start_dt <= log_time <= end_dt):
                continue

            if use_regex:
                try:
                    if not re.search(query, line):
                        continue
                except re.error:
                    return jsonify({"error": "Invalid regex"}), 400
            else:
                if query.lower() not in line.lower():
                    continue

            filtered_logs.append({
                "timestamp": ts,
                "message": line.strip()
            })
           

    return jsonify(filtered_logs)
