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

import requests
from kafka import KafkaConsumer


def setup_logging(script_name: str, log_dir: str = "./logs", level: str = "DEBUG") -> logging.Logger:
    log_level = getattr(logging, level.upper(), logging.INFO)
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    if logger.handlers:
        return logger  # Prevent duplicate handlers

    formatter = logging.Formatter("%(asctime)s [%(name)s:%(threadName)s:%(funcName)s] %(levelname)s: %(message)s")

    # File handler
    log_file = log_path / f"{script_name}.log"
    file_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


stop_event = threading.Event()
signal.signal(signal.SIGINT, lambda sig, frame: stop_event.set())
signal.signal(signal.SIGTERM, lambda sig, frame: stop_event.set())
script_name = Path(sys.argv[0]).stem
LOGLEVEL = os.environ.get("LOGLEVEL", "DEBUG")
logger = setup_logging(script_name, log_dir="./logs", level=LOGLEVEL)


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka → REST pipeline")
    parser.add_argument("--domain", default="IPCORE", help="Domain name from config.json")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    return parser.parse_args()


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


def save_file(filename: Path, record: list[dict]):
    filename = Path(filename)
    logger.info(f"Adding records to {filename}")
    with filename.open("a", encoding="utf-8") as f:
        for entry in record:
            f.write(json.dumps(entry) + "\n")
    logger.info(f"File {filename} updated with {len(record)} entries")


def add_to_batch_file(batch_file_name: Path, record: list[dict]):
    save_file(batch_file_name, record)


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
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    transformed_batch = []
    raw_batch = []

    last_flush = time.time()

    try:
        for message in consumer:
            retry_failed_files(app_config.aiops, app_config)
            if stop_event.is_set():
                logger.info("Shutdown signal received. Exiting Kafka loop.")
                break
            msg_json = message.value
            print(msg_json)
            raw_batch.append(msg_json)
            print(len(raw_batch))
            transformed_payload = transform_message(msg_json, app_config)

            if not transformed_payload:
                logging.warning("Skipping untransformable message")
                save_malformed_message(app_config, msg_json)
                continue

            transformed_batch.append(transformed_payload)
            if len(raw_batch) >= app_config.kafka.BATCH_SIZE or (time.time() - last_flush) > app_config.kafka.BATCH_TIMEOUT_SECONDS:
                logger.info(f"Batch size exceeded {len(raw_batch)}. Processing")
                ts = datetime.now().strftime("%Y%m%d%H%M%S_%f")
                raw_file = app_config.INCOMING_DIR / f"raw_{ts}.json"
                processing_file = app_config.PROCESSING_DIR / f"processing_{ts}.json"
                add_to_batch_file(raw_file, raw_batch)
                add_to_batch_file(processing_file, transformed_batch)
                send_to_api(app_config.aiops, app_config)
                logger.info("Finished processing.")
                quit()
                consumer.commit()
                raw_batch.clear()
                transformed_batch.clear()
                last_flush = time.time()
            else:
                logger.info(f"Batch size not exceeded {len(raw_batch)}. Waiting for more data")

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


def transform_message(entry: dict, app_config: AppConfig):
    def get_value(name):
        return next(
            (item["value"] for item in entry.get("data", []) if item.get("name") == name),
            None,
        )


    if app_config.script.METRIC_TIMESTAMP_FIELD not in entry:
        logger.error(f"The data is missing timestamp field {entry}")
        return False
    else:
        raw_timestamp = entry.get(app_config.script.METRIC_TIMESTAMP_FIELD)

    # Normalize timestamp (convert to ms if it's in seconds)
    timestamp = int(raw_timestamp)
    if len(str(timestamp)) == 10:
        timestamp *= 1000

    unique_fields=domain_transform(app_config.script.DOMAIN,entry)
    resource_id=unique_fields.get("resource_id")
    node_id=unique_fields.get("node")
    if not resource_id:
        logger.error("Dropping message due to Missing resourceID fields")
        return None
    if not node_id:
        logger.error("Dropping message due to Missing Node fields")
        return None

    metric_keys = app_config.script.METRIC_KEYS
    metrics = {key.replace(" ", "_"): get_value(key) for key in metric_keys}
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
                    time.sleep(min(2**attempt, 30))

            except Exception as e:
                logger.error(f"Retry error on {file_path.name}, attempt {attempt}: {e}")
                time.sleep(min(2**attempt, 30))

        if not success:
            logger.error(f"Retry failed for {file_path.name} after {aiops_config.MAX_RETRIES} attempts")
            new_filename = file_path.name.replace("processing", "retry_failed")
            dest = Path(script_config.FAILED_DIR) / new_filename
            logger.error(f"Renaming file {file_path.name} to {dest}")
            shutil.move(str(file_path), dest)


def post_metrics(aiops_config: AIOpsConfig, payload: dict) -> requests.Response:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ZenApiKey {aiops_config.API_KEY}",
        "X-TenantID": aiops_config.TENANT_ID,
    }
    timeout = 30
    return requests.post(
        f"{aiops_config.AIOPS_HOST}/{aiops_config.METRICS_API_URI}",
        headers=headers,
        json=payload,
        timeout=timeout,
        verify=False
    )


def send_to_api(aiops_config: AIOpsConfig, script_config: AppConfig):
    processing_dir = Path(script_config.PROCESSING_DIR)
    files = sorted(processing_dir.glob("*.json"))
    if not files:
        logger.info("No files found to process.")
        return False
    for file_path in files:
        logger.info(f"Sending file {file_path} to API")

        success = False
        for attempt in range(1, aiops_config.MAX_RETRIES + 1):
            try:
                json_lines = read_json_lines(file_path)

                resp = post_metrics(aiops_config, payload={"groups": json_lines})

                if resp.status_code == 200:
                    new_filename = file_path.name.replace("processing_", "processed_")
                    dest = Path(script_config.PROCESSED_DIR) / new_filename
                    logger.info(f"POST success: {file_path.name}. Moving file from {str(file_path)} to {dest}")
                    shutil.move(str(file_path), dest)
                    success = True
                    break
                else:
                    logging.warning(f"POST failed (status {resp.status_code}) on {file_path.name}, attempt {attempt}")
                    time.sleep(min(2**attempt, 30))

            except Exception as e:
                logging.error(f"POST error on {file_path.name}, attempt {attempt}: {e}")
                time.sleep(min(2**attempt, 30))

            time.sleep(2)

        if not success:
            logging.error(f"Giving up on file after {aiops_config.MAX_RETRIES} attempts: {file_path.name}")

            dest = Path(script_config.FAILED_DIR) / file_path.name
            logging.error(f"Moving file from {str(file_path)} to {dest}")
            shutil.move(str(file_path), dest)

    return True


def resilient_kafka_loop(app_config):
    while not stop_event.is_set():
        try:
            kafka_consumer(app_config)
        except Exception:
            logger.exception("Kafka consumption failed. Retrying in 5 seconds...")
            time.sleep(5)


def main():
    logger.info("Starting application")

    args = parse_args()
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

    try:
        perform_disk_cleanup(config)
        resilient_kafka_loop(config)
    except NotImplementedError as e:
        logger.warning(str(e))

    logger.info("All threads completed. Exiting.")


if __name__ == "__main__":
    main()
