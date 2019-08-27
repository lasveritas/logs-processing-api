import json
import time
import argparse
from flask import Flask, request, jsonify
from logs_service import LogsService


def create_app(query_index, logs_index):
    app = Flask(__name__)
    app.config["query_index"] = query_index
    app.config["logs_index"] = logs_index
    ls = LogsService(
        app.logger, app.config["query_index"], app.config["logs_index"])

    @app.route("/1/queries/count/<date_prefix>")
    def get_distinct_queries_count(date_prefix):
        count = ls.get_distinct_queries_count(date_prefix)
        
        return jsonify(count)

    @app.route("/1/queries/popular/<date_prefix>")
    def get_top_popular_queries(date_prefix):
        size = request.args.get("size", "3")
        top_queries = ls.get_top_popular_queries(date_prefix, size)

        return jsonify(top_queries)

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query_index", type=argparse.FileType("r"), help="file with logs index")
    parser.add_argument(
        "--logs_index", type=argparse.FileType("r"), help="file with query index")

    args = parser.parse_args()

    print("Loading indexes...")
    start = time.time()
    query_index = json.load(args.query_index)
    logs_index = json.load(args.logs_index)
    end = time.time()
    print(f"Done in {end-start} s")

    app = create_app(query_index, logs_index)
    app.run()
