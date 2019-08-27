import json
import time
import argparse
from collections import defaultdict


def get_query_data_and_query_index(raw_logs_file):
    """
    query_data: <query_str>: {"id": <query_id>, "dates": {<date_str>: [<time_str>]}, "unique": bool }
    id_query_index: <query_id>: <query_str>
    """
    query_dates_data = {}
    id_query_index = {}

    for line in raw_logs_file:
        line = line.strip().split("\t")
        query = line[1]
        date, time = line[0].split()

        if query not in query_dates_data:
            query_id = len(query_dates_data)
            query_dates_data[query] = {
                "id": query_id,
                "dates": defaultdict(list),
                "unique": True,
            }
            id_query_index[query_id] = query

        query_dates_data[query]["dates"][date].append(time)
        if len(query_dates_data[query]["dates"]) > 1:
            query_dates_data[query]["unique"] = False

    return query_dates_data, id_query_index


def get_logs_index(query_data):
    logs_index = {}
    for query, query_data in query_data.items():
        query_id = query_data["id"]
        logs_key = "unique_logs" if query_data["unique"] else "frequent_logs"
        for date in query_data["dates"]:
            year, month, day = date.split("-")
            if year not in logs_index:
                logs_index[year] = {}
            if month not in logs_index[year]:
                logs_index[year][month] = {}
            if day not in logs_index[year][month]:
                logs_index[year][month][day] = {"frequent_logs": {}, "unique_logs": {}}

            if query_id not in logs_index[year][month][day][logs_key]:
                logs_index[year][month][day][logs_key][query_id] = {"_count": 0}

            for time in query_data["dates"][date]:
                hour, minute, second = time.split(":")

                if hour not in logs_index[year][month][day][logs_key][query_id]:
                    logs_index[year][month][day][logs_key][query_id][hour] = {
                        "_count": 0
                    }
                if minute not in logs_index[year][month][day][logs_key][query_id][hour]:
                    logs_index[year][month][day][logs_key][query_id][hour][minute] = 0

                logs_index[year][month][day][logs_key][query_id][hour][minute] += 1
                logs_index[year][month][day][logs_key][query_id][hour]["_count"] += 1
                logs_index[year][month][day][logs_key][query_id]["_count"] += 1

    return logs_index


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build logs index.")
    parser.add_argument(
        "--raw_logs", type=argparse.FileType("r"), help="input file with logs"
    )
    parser.add_argument(
        "--logs_index", type=argparse.FileType("w"), help="output file for logs index"
    )
    parser.add_argument(
        "--query_index", type=argparse.FileType("w"), help="output file for query index"
    )

    args = parser.parse_args()

    raw_logs_file = args.raw_logs
    logs_index_file = args.logs_index
    query_index_file = args.query_index

    print("Building query index...")
    start = time.time()
    query_data, id_query_index = get_query_data_and_query_index(raw_logs_file)
    end = time.time()
    print(f"Done in {end-start} s")
    print("Building logs index...")
    start = time.time()
    logs_index = get_logs_index(query_data)
    end = time.time()
    print(f"Done in {end-start} s")

    print("Writing indexes to files...")
    query_index_file.write(json.dumps(id_query_index))
    logs_index_file.write(json.dumps(logs_index))
    print(f"Query index in file: {query_index_file.name}")
    print(f"Logs index in file: {logs_index_file.name}")
