#!/usr/bin/env python3
"""
Process kafka messages for AIOps Metrics API
"""

import argparse
import json
import logging
import os
import shutil
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

import requests
from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition


def setup_logging(log_file_name: str, log_dir: str = "./logs", level: str = "DEBUG") -> logging.Logger:
    log_level = getattr(logging, level.upper(), logging.INFO)
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    logger_name = logging.getLogger(__name__)
    logger_name.setLevel(log_level)
    if logger_name.handlers:
        return logger_name  # Prevent duplicate handlers
    formatter = logging.Formatter("%(asctime)s [%(name)s:%(threadName)s:%(funcName)s] %(levelname)s: %(message)s")
    # File handler
    log_file = log_path / f"{log_file_name}.log"

    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(formatter)
    logger_name.addHandler(file_handler)

    # Console handler for testing. remove later
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger_name.addHandler(console_handler)
    return logger_name


stop_event = threading.Event()
signal.signal(signal.SIGINT, lambda sig, frame: stop_event.set())
signal.signal(signal.SIGTERM, lambda sig, frame: stop_event.set())
script_name = Path(sys.argv[0]).stem
LOGLEVEL = os.environ.get("LOGLEVEL", "DEBUG")
logger: Optional[logging.Logger] = None

batch_queue = Queue()
offset_commit_queue = Queue()


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka → REST pipeline")
    parser.add_argument("--domain", default="IPCORE", help="Domain name from config.json")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    return parser.parse_args()


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaConfig:
    KAFKA_BROKER: list[str]
    TOPIC: str
    GROUP_ID: str
    BATCH_SIZE: int
    BATCH_TIMEOUT_SECONDS: int


@dataclass(frozen=True)
class AIOpsConfig:
    AIOPS_HOST: str
    API_KEY: str
    TENANT_ID: str
    METRICS_API_URI: str
    BASE_TIMEOUT: int = 1000
    BACKOFF_FACTOR: int = 2
    MAX_RETRIES: int = 3


@dataclass(frozen=True)
class ScriptConfig:
    DOMAIN: str
    BASE_DIR: Path
    METRIC_KEYS: list[str]
    METRIC_GROUP_ID: str
    METRIC_TIMESTAMP_FIELD: str


@dataclass
class AppConfig:
    kafka: KafkaConfig
    aiops: AIOpsConfig
    script: ScriptConfig

    @property
    def BASE_DIR(self) -> Path:
        return self.script.BASE_DIR

    @property
    def DOMAIN(self) -> str:
        return self.script.DOMAIN

    @property
    def INCOMING_DIR(self) -> Path:
        return self.BASE_DIR / "incoming"

    @property
    def PROCESSED_DIR(self) -> Path:
        return self.BASE_DIR / "processed"

    @property
    def PROCESSING_DIR(self) -> Path:
        return self.BASE_DIR / "processing"

    @property
    def FAILED_DIR(self) -> Path:
        return self.BASE_DIR / "failed"

    def ensure_dirs(self) -> None:
        for path in [
            self.INCOMING_DIR,
            self.PROCESSED_DIR,
            self.FAILED_DIR,
            self.PROCESSING_DIR,
        ]:
            path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load(cls, domain_arg: str, path: str = "config.json") -> "AppConfig":
        try:
            with open(path, encoding="utf-8") as f:
                cfg = json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file '{path}' not found.")
            sys.exit(1)

        try:
            domain_cfg = cfg["scriptConfig"][domain_arg]
        except KeyError:
            logger.error(f"Domain '{domain_arg}' not found in config.json")
            sys.exit(1)

        script_cfg = ScriptConfig(
            DOMAIN=domain_arg,
            BASE_DIR=Path(domain_cfg["BASE_DIR"]),
            METRIC_KEYS=domain_cfg["METRIC_KEYS"],
            METRIC_GROUP_ID=domain_cfg["METRIC_GROUP_ID"],
            METRIC_TIMESTAMP_FIELD=domain_cfg["METRIC_TIMESTAMP_FIELD"],
        )

        app_config = cls(
            kafka=KafkaConfig(**cfg["kafka"]),
            aiops=AIOpsConfig(**cfg["aiops"]),
            script=script_cfg,
        )
        app_config.ensure_dirs()
        return app_config


def append_json_lines_to_file(filename: Path, record: list[dict]):
    filename = Path(filename)
    logger.info(f"Adding records to {filename}")
    with filename.open("a", encoding="utf-8") as f:
        for entry in record:
            f.write(json.dumps(entry) + "\n")
    logger.info(f"File {filename} updated with {len(record)} entries")


def save_file(filename: Path, record: dict):
    filename = Path(filename)
    logger.debug(f"Adding records to {filename}")
    with filename.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def cleanup_old_files(directory: Path, days: int = 7):
    now = datetime.now()
    threshold = now - timedelta(days=days)
    if not directory.exists():
        return
    for file in directory.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(file.stat().st_mtime)
            if mtime < threshold:
                file.unlink()
                logger.info(f"Deleted old file: {file}")
        except Exception as e:
            logger.warning(f"Failed to delete {file}: {e}")


def perform_disk_cleanup(config: AppConfig, days: int = 7):
    cleanup_old_files(config.PROCESSED_DIR, days)
    cleanup_old_files(config.FAILED_DIR, days)
    cleanup_old_files(config.INCOMING_DIR, days)
    cleanup_old_files(config.PROCESSING_DIR, days)


def save_malformed_message(app_config: AppConfig, entry: dict) -> None:
    timestamp = datetime.now().strftime("%Y%m%d%H")
    filename = app_config.FAILED_DIR / f"malformed_{timestamp}.json"
    try:
        with filename.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
        logger.info(f"Saved malformed message to {filename}")
    except Exception as e:
        logger.error(f"Failed to save malformed message: {e}")


def kafka_consumer(app_config: AppConfig):
    logger.info(f"Consuming Kafka messages from {app_config.kafka.KAFKA_BROKER} Topic: {app_config.kafka.TOPIC}")

    consumer = KafkaConsumer(
        app_config.kafka.TOPIC,
        bootstrap_servers=app_config.kafka.KAFKA_BROKER,
        group_id=app_config.kafka.GROUP_ID,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        auto_offset_reset="earliest",
        max_poll_interval_ms=300000,  # 5 minutes
        session_timeout_ms=10000,  # 10 seconds
        heartbeat_interval_ms=3000,  # Optional, must be < session_timeout_ms
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    last_flush = time.time()
    buffer = []
    try:
        for message in consumer:
            retry_failed_files(app_config.aiops, app_config)
            if stop_event.is_set():
                logger.info("Shutdown signal received. Exiting Kafka loop.")
                break
            msg_json = message.value

            transformed_payload = transform_kafka_message_to_metric_payload(msg_json, app_config)
            if not transformed_payload:
                logging.warning(f"Skipping malformed message {msg_json}")
                save_malformed_message(app_config, msg_json)
                continue
            buffer.append((message, transformed_payload))
            now = time.time()
            time_elapsed = now - last_flush

            # if len(raw_batch) >= app_config.kafka.BATCH_SIZE or (time.time() - last_flush) > app_config.kafka.BATCH_TIMEOUT_SECONDS:
            if len(buffer) >= app_config.kafka.BATCH_SIZE or (time_elapsed) >= app_config.kafka.BATCH_TIMEOUT_SECONDS:
                logger.info(f"Batch size exceeded {len(buffer)}.Processing")
                last_msg = buffer[-1][0]
                offset_info = (last_msg.topic, last_msg.partition, last_msg.offset)
                batch_queue.put((buffer.copy(), offset_info))
                buffer.clear()
                logger.info("Finished processing.")
                last_flush = time.time()
                while True:
                    try:
                        topic, partition, offset = offset_commit_queue.get(timeout=1)
                        tp = TopicPartition(topic, partition)
                        metadata = OffsetAndMetadata(offset=offset + 1, metadata=None, leader_epoch=-1)
                        consumer.commit(offsets={tp: metadata})
                        offset_commit_queue.task_done()
                    except Empty:
                        logger.debug("No response from commit queue, let's break it")
                        break
                    except Exception as e:
                        logger.error(f"Offset commint failed {e}")
            else:
                logger.info(f"Batch size not exceeded {len(buffer)}. Waiting for more data")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
    except KeyboardInterrupt:
        logger.info("Shutting down consumer due to keyboard interrupt")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise
    finally:
        logger.info("Closing Kafka consumer")
        consumer.close()


def domain_transform(domain_name: str, message: dict):
    def ipcore(message: dict):
        record_type = message.get("recordTypeName")
        if "networkElementPath" not in message:
            logger.error("Data is missing networkElementPath")
            return None, None
        network_element_list = [p.strip() for p in message.get("networkElementPath", "").split(">")]

        if record_type == "interface":
            resource_id = "_".join(network_element_list[-2:])
            node = network_element_list[-2]

        else:
            resource_id = network_element_list[-1]
            node = network_element_list[-2]

        return resource_id, node

    if domain_name == "IPCORE":
        resource_id, node = ipcore(message)

        return {"resource_id": resource_id, "node": node}


def transform_kafka_message_to_metric_payload(entry: dict, app_config: AppConfig):
    def get_value(name):
        return next(
            (item["value"] for item in entry.get("data", []) if item.get("name") == name),
            None,
        )

    if app_config.script.METRIC_TIMESTAMP_FIELD not in entry:
        logger.error(f"The data is missing timestamp field {entry}")
        return None
    else:
        raw_timestamp = entry.get(app_config.script.METRIC_TIMESTAMP_FIELD)

    # Normalize timestamp (convert to ms if it's in seconds)
    timestamp = int(raw_timestamp)
    if len(str(timestamp)) == 10:
        timestamp *= 1000

    unique_fields = domain_transform(app_config.script.DOMAIN, entry)
    resource_id = unique_fields.get("resource_id")
    node_id = unique_fields.get("node")
    if not resource_id:
        logger.error("Dropping message due to Missing resourceID fields")
        return None
    if not node_id:
        logger.error("Dropping message due to Missing Node fields")
        return None

    metric_keys = app_config.script.METRIC_KEYS
    metrics = {}
    for key in metric_keys:
        val = get_value(key)
        if val is None:
            logger.warning(f"Missing metric key: {key}")
        metrics[key.replace(" ", "_")] = val
    if not metrics:
        logger.error("Dropping message due to Missing metrics")
        return None

    return {
        "timestamp": timestamp,
        "resourceID": resource_id,
        "attributes": {"group": app_config.script.METRIC_GROUP_ID, "node": node_id},
        "metrics": metrics,
    }


def read_json_lines(file_path: Path) -> list[dict]:
    try:
        with file_path.open("rb") as f:
            return [json.loads(line.strip()) for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Failed to read/parse {file_path}: {e}")
        return []


def retry_failed_files(aiops_config: AIOpsConfig, script_config: AppConfig):
    failed_dir = Path(script_config.FAILED_DIR)
    files = sorted(failed_dir.glob("processing*.json"))

    if not files:
        logger.debug("No failed files to retry.")
        return

    logger.info(f"Retrying {len(files)} failed files...")

    for file_path in files:
        logger.info(f"Retrying failed file: {file_path.name}")
        success = False

        for attempt in range(1, aiops_config.MAX_RETRIES + 1):
            try:
                json_lines = read_json_lines(file_path)
                payload = {"groups": json_lines}
                resp = post_metrics(aiops_config, payload=payload)

                if resp.status_code == 200:
                    new_filename = file_path.name.replace("processing", "processed_retry")
                    dest = Path(script_config.PROCESSED_DIR) / new_filename
                    logger.info(f"Retry success. Moving {file_path} → {dest}")
                    shutil.move(str(file_path), dest)
                    success = True
                    break
                else:
                    logger.warning(f"Retry failed (status {resp.status_code}) for {file_path.name}, attempt {attempt}")
                    delay = min(aiops_config.BASE_TIMEOUT * (aiops_config.BACKOFF_FACTOR ** (attempt - 1)), 30)
                    time.sleep(delay)

            except Exception as e:
                logger.error(f"Retry error on {file_path.name}, attempt {attempt}: {e}")
                delay = min(aiops_config.BASE_TIMEOUT * (aiops_config.BACKOFF_FACTOR ** (attempt - 1)), 30)
                time.sleep(delay)

        if not success:
            logger.error(f"Retry failed for {file_path.name} after {aiops_config.MAX_RETRIES} attempts")
            new_filename = file_path.name.replace("processing", "retry_failed")
            dest = Path(script_config.FAILED_DIR) / new_filename
            logger.error(f"Renaming file {file_path.name} to {dest}")
            shutil.move(str(file_path), dest)


def metrics_post_worker(aiops_config: AIOpsConfig, script_config: AppConfig):
    while not stop_event.is_set():
        try:
            try:
                batch, last_offset_info = batch_queue.get(timeout=1)
            except Empty:
                continue
            raw_records = []
            transformed_records = []

            for msg, transformed in batch:
                raw_records.append(msg.value)
                transformed_records.append(transformed)

            payload = {"groups": transformed_records}

            timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
            processing_file_name = f"processing_{timestamp_str}.json"
            processing_path = script_config.PROCESSING_DIR / processing_file_name

            raw_input_backup_file = script_config.INCOMING_DIR / processing_file_name.replace("processing_", "raw_")
            # we are not saving payload , we are saving th transfored file. have to send payload.
            append_json_lines_to_file(processing_path, transformed_records)
            append_json_lines_to_file(raw_input_backup_file, raw_records)

            success = False
            for attempt in range(aiops_config.MAX_RETRIES):
                try:
                    resp = post_metrics(aiops_config, payload)
                    if 200 <= resp.status_code < 300:
                        success = True
                        break
                    else:
                        raise Exception(f"HTTP {resp.status_code} : {resp.text}")
                except Exception as e:
                    logger.warning(f"[POST WORKER] Attempt {attempt + 1} failed: {e}")
                    delay = min(aiops_config.BASE_TIMEOUT * (aiops_config.BACKOFF_FACTOR ** (attempt - 1)), 30)
                    time.sleep(delay)

            if success:
                # Move to processed
                processed_path = script_config.PROCESSED_DIR / processing_file_name.replace("processing_", "processed_")
                shutil.move(str(processing_path), processed_path)
                logger.info(f"[POST WORKER] Success. Moved to {processed_path}.offset ={last_offset_info}")
                offset_commit_queue.put(last_offset_info)
            else:
                # Move to failed
                failed_path = script_config.FAILED_DIR / processing_file_name
                shutil.move(str(processing_path), failed_path)
                logger.error(f"[POST WORKER] Failed after retries. Moved to {failed_path}")

            batch_queue.task_done()

        except Exception as e:
            logger.exception(f"[POST WORKER] Unhandled error in thread: {e}")


def post_metrics(aiops_config: AIOpsConfig, payload) -> requests.Response:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ZenApiKey {aiops_config.API_KEY}",
        "X-TenantID": aiops_config.TENANT_ID,
    }
    timeout = 30
    return requests.post(f"{aiops_config.AIOPS_HOST}/{aiops_config.METRICS_API_URI}", headers=headers, json=payload, timeout=timeout, verify=False)


def start_kafka_consumer(app_config: AppConfig):
    while not stop_event.is_set():
        try:
            kafka_consumer(app_config)
        except Exception:
            logger.exception("[KAFKA LOOP] Kafka consumer crashed. Retrying in 5 seconds...")
            time.sleep(5)


def main():
    args = parse_args()
    global logger
    logger = setup_logging(f"{script_name}_{args.domain.lower()}", log_dir="./logs", level=LOGLEVEL)
    logger.info("Starting application")

    config = AppConfig.load(args.domain, args.config)

    logger.debug(
        "Loaded config:\n%s",
        json.dumps(
            {
                "DOMAIN": config.DOMAIN,
                "BASE_DIR": str(config.BASE_DIR),
                "kafka": config.kafka.__dict__,
                "aiops": config.aiops.__dict__,
                "config": str(config),
            },
            indent=2,
        ),
    )
    consumer_thread = threading.Thread(target=start_kafka_consumer, args=(config,), daemon=True, name="KafkaConsumerThread")

    consumer_thread.start()
    for _ in range(3):
        t = threading.Thread(target=metrics_post_worker, args=(config.aiops, config), daemon=True)
        t.start()
    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        batch_queue.join()
        offset_commit_queue.join()

    # try:
    #     perform_disk_cleanup(config)
    #     resilient_kafka_loop(config)
    # except NotImplementedError as e:
    #     logger.warning(str(e))

    logger.info("All threads completed. Exiting.")


if __name__ == "__main__":
    main()
