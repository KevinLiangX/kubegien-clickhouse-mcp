# -*- coding: utf-8 -*-
import requests
import uuid
import time
import random
import json
from datetime import datetime, timedelta

# 配置信息
CLICKHOUSE_URL = "http://localhost:8123/"
# 配置信息
CLICKHOUSE_URL = "http://localhost:8123/"
DB_NAME = "logs"

# 写入目标表
# 生产环境通常写入分布式表 (log_file_all)，由 ClickHouse 自动分发数据到各个分片。
# 单机环境如果没有配置集群导致无法创建 Distributed 表，请改为 "log_file_local"。
TABLE_NAME = "log_file_all" 

USER = "default"
PASSWORD = "password"

# 模拟数据池
CLUSTERS = ["cluster-01", "cluster-02"]
NODES = ["node-01", "node-02", "node-03", "node-04"]
NAMESPACES = ["default", "kube-system", "monitoring", "logging"]
APPS = {
    "default": ["nginx", "redis", "backend-api"],
    "kube-system": ["coredns", "kube-proxy"],
    "monitoring": ["prometheus", "grafana"],
    "logging": ["fluent-bit", "elasticsearch"]
}
# 丰富日志内容配置
LOG_TEMPLATES = {
    "INFO": [
        "| {context_id} | {trace_id} | {long_txn_id} | INFO {class_name} | Request received: method={method} path={path} status={status} duration={duration}ms",
        "| {context_id} | {trace_id} | {long_txn_id} | INFO {class_name} | Service started successfully on port {port}",
        "| {context_id} | {trace_id} | {long_txn_id} | INFO {class_name} | 流程 [{process_id}] 执行开始，请求参数:\n{json_payload}",
        "| {context_id} | {trace_id} | {long_txn_id} | INFO {class_name} | Job execution completed: job_id={job_id} status=SUCCESS"
    ],
    "WARN": [
        "| {context_id} | {trace_id} | {long_txn_id} | WARN {class_name} | High memory usage detected: {usage}% used",
        "| {context_id} | {trace_id} | {long_txn_id} | WARN {class_name} | Response time threshold exceeded: {duration}ms > 500ms"
    ],
    "ERROR": [
        "| {context_id} | {trace_id} | {long_txn_id} | ERROR {class_name} | Database connection failed: {db_error}",
        "| {context_id} | {trace_id} | {long_txn_id} | ERROR {class_name} | Payment processing failed for transaction {long_txn_id}: {payment_error}",
        "| {context_id} | {trace_id} | {long_txn_id} | ERROR {class_name} | Critical system failure: code={error_code} message={error_msg}"
    ],
    "DEBUG": [
        "| {context_id} | {trace_id} | {long_txn_id} | DEBUG {class_name} | Payload received: {json_payload}",
        "| {context_id} | {trace_id} | {long_txn_id} | DEBUG {class_name} | Entering method: {class_name}.{method_name} with args={args}"
    ]
}

# 辅助数据生成
METHODS = ["GET", "POST", "PUT", "DELETE"]
PATHS = ["/api/v1/users", "/api/v1/orders", "/health", "/login", "/static/css/style.css"]
STATUS_CODES = [200, 201, 204, 301, 304]
COMPONENTS = ["db-connector", "cache-layer", "auth-service"]
DB_ERRORS = ["Connection refused", "Too many connections", "Deadlock found"]
PAYMENT_ERRORS = ["Insufficient funds", "Card expired", "Fraud detected"]

def generate_message(level):
    template = random.choice(LOG_TEMPLATES[level])
    
    trace_id = f"T{uuid.uuid4().hex}"
    long_txn_id = f"500{random.randint(20250101000000000000, 20251231999999999999)}"
    context_id = f"c{random.randint(100000, 999999)}"
    
    # 动态填充模板变量
    data = {
        "context_id": context_id,
        "trace_id": trace_id,
        "long_txn_id": long_txn_id,
        "process_id": str(random.randint(1000000, 9999999)),
        "method": random.choice(METHODS),
        "path": random.choice(PATHS),
        "status": random.choice(STATUS_CODES),
        "duration": random.randint(5, 2000),
        "ip": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
        "port": random.randint(8000, 9000),
        "component": random.choice(COMPONENTS),
        "size": random.randint(10, 500),
        "user_id": random.randint(1000, 9999),
        "session_id": str(uuid.uuid4())[:8],
        "job_id": f"job-{random.randint(100, 999)}",
        "usage": random.randint(70, 99),
        "active": random.randint(90, 100),
        "max": 100,
        "attempt": random.randint(1, 5),
        "volume": "/data",
        "db_error": random.choice(DB_ERRORS),
        "class_name": "com.crb.core.processor.CoreProcessor",
        "method_name": "processRequest",
        "line_num": random.randint(10, 500),
        "txn_id": f"txn_{random.randint(10000, 99999)}",
        "payment_error": random.choice(PAYMENT_ERRORS),
        "service_name": "user-service",
        "file_path": "/var/log/app.log",
        "error_code": "E500",
        "error_msg": "Internal Server Error",
        "json_payload": json.dumps({
            "sysHead": {
                "chnlTranDate": "2025-11-07 00:00:00",
                "consumerId": "1038",
                "orgSysId": "0302",
                "reqTranTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "seqNo": f"L0302{random.randint(1000000000000, 9999999999999)}",
                "serviceCode": "94002030001",
                "serviceScene": "03",
                "tranDate": "2025-11-07 00:00:00"
            },
            "sysReqHead": {
                "traceId": trace_id
            },
            "appHead": {
                "appFlag": "",
                "authFlag": "",
                "busiId": "001"
            }
        }, indent=4, ensure_ascii=False),
        "keys": "last_login, preferences",
        "x": random.randint(0, 100),
        "y": random.randint(0, 100),
        "state": "ACTIVE",
        "args": "id=123",
        "cache_key": f"user:{random.randint(1000, 9999)}"
    }
    
    # 使用 safe format 避免 key error (简单起见，这里假设模板key都在data里)
    try:
        return template.format(**data)
    except KeyError:
        return template # Fallback

def generate_log_entry():
    namespace = random.choice(NAMESPACES)
    app_name = random.choice(APPS[namespace])
    now = datetime.now()
    
    # 按照一定概率分布生成日志级别
    rand_val = random.random()
    if rand_val < 0.7: level = "INFO"
    elif rand_val < 0.85: level = "WARN"
    elif rand_val < 0.95: level = "ERROR"
    else: level = "DEBUG"
    
    message_content = generate_message(level)
    
    # 构造符合 schema 的数据
    return {
        "id": str(uuid.uuid4()),
        "createdtime": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "dbcreatedtime": now.strftime("%Y-%m-%d %H:%M:%S"),
        "expiretime": (now + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S"),
        "cluster_name": random.choice(CLUSTERS),
        "node_name": random.choice(NODES),
        "app_name": app_name,
        "namespace": namespace,
        "workload_type": "Deployment",
        "workload_name": f"{app_name}-deployment",
        "log_name": f"{app_name}.log",
        "logpath_name": f"/var/log/containers/{app_name}_{namespace}_{uuid.uuid4()}.log",
        "file_offset": random.randint(1000, 1000000),
        "message": f"[{level}] {now.strftime('%Y-%m-%d %H:%M:%S')} {message_content}"
    }

def insert_logs(batch_size=10):
    data = []
    for _ in range(batch_size):
        data.append(generate_log_entry())
    
    # 使用 JSONEachRow 格式插入
    query = f"INSERT INTO {DB_NAME}.{TABLE_NAME} FORMAT JSONEachRow"
    
    # 将数据转换为 NDJSON (每行一个 JSON 对象)
    payload = "\n".join([json.dumps(row) for row in data])
    
    try:
        response = requests.post(
            CLICKHOUSE_URL,
            params={"query": query},
            data=payload,
            auth=(USER, PASSWORD)
        )
        
        if response.status_code == 200:
            print(f"Successfully inserted {batch_size} records.")
        else:
            print(f"Failed to insert records. Status: {response.status_code}, Error: {response.text}")
            
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Simulate Fluent Bit log injection into ClickHouse")
    parser.add_argument("--count", type=int, default=100, help="Total number of logs to insert")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of logs per insert request")
    parser.add_argument("--interval", type=float, default=0.0, help="Interval between batches in seconds (0 for no delay)")
    parser.add_argument("--loop", action="store_true", help="Run continuously until interrupted")
    
    args = parser.parse_args()
    
    print(f"Starting log injection to {CLICKHOUSE_URL}...")
    
    total_inserted = 0
    try:
        while True:
            current_batch = args.batch_size
            if not args.loop:
                remaining = args.count - total_inserted
                if remaining <= 0:
                    break
                current_batch = min(remaining, args.batch_size)
            
            insert_logs(current_batch)
            total_inserted += current_batch
            
            if args.interval > 0:
                time.sleep(args.interval)
                
            if not args.loop and total_inserted >= args.count:
                break
                
    except KeyboardInterrupt:
        print("\nStopped by user.")
    
    print(f"Done. Total inserted: {total_inserted}")
