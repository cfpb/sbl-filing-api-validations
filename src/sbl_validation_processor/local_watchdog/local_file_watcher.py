import time
import json

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from sbl_validation_processor.csv_to_parquet import split_csv_into_parquet
from sbl_validation_processor.parquet_validator import validate_parquets
from sbl_validation_processor.results_aggregator import aggregate_validation_results

local_path = "/tmp/filing_bucket/upload/"


class CsvHandler(PatternMatchingEventHandler):
    patterns = ["*.csv"]

    def on_created(self, event):
        print(f"CSV File created: {event.src_path}", flush=True)
        if "report.csv" not in event.src_path:
            split_csv_into_parquet(local_path, event.src_path.replace(local_path, ""))
            paths = event.src_path.split("/")
            fname = paths[-1]
            with open(
                "/".join(paths[:-1]) + f"/{fname.split(".")[0]}.done_pqs", "wb"
            ) as pqs_file:
                pqs_file.write(f"{fname} to parquet done".encode("utf-8"))


class PqsHandler(PatternMatchingEventHandler):
    patterns = ["*.done_pqs"]

    def on_created(self, event):
        print(f"PQS File created: {event.src_path}", flush=True)
        paths = event.src_path.split("/")
        sub_id = paths[-1].split(".")[0]
        key = "/".join(paths[:-1]) + f"/{sub_id}_pqs/"
        response = validate_parquets(local_path, key.replace(local_path, ""))

        with open("/".join(paths[:-1]) + f"/{sub_id}.done_res", "wb") as res_file:
            res_file.write(json.dumps(response).encode("utf-8"))


class ResHandler(PatternMatchingEventHandler):
    patterns = ["*.done_res"]

    def on_created(self, event):
        print(f"RES File created: {event.src_path}", flush=True)
        with open(event.src_path, "r") as file:
            results = json.loads(file.read())
        paths = event.src_path.split("/")
        fname = paths[-1].split(".")[0]
        key = "/".join(paths[:-1]) + f"/{fname}_res/"
        aggregate_validation_results(local_path, key.replace(local_path, ""), results)


if __name__ == "__main__":
    csv_event_handler = CsvHandler()
    pqs_event_handler = PqsHandler()
    res_event_hander = ResHandler()

    observer = Observer()
    observer.schedule(csv_event_handler, path=local_path, recursive=True)
    observer.schedule(pqs_event_handler, path=local_path, recursive=True)
    observer.schedule(res_event_hander, path=local_path, recursive=True)

    observer.start()
    print("Observer started, looping", flush=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    print("Done, killing observer", flush=True)
    observer.join()
