import re
import json
import heapq
import datetime
from operator import itemgetter
from collections import namedtuple, Counter


class LogsService:
    def __init__(self, logger, query_index, logs_index):
        self.logger = logger
        self.query_index = query_index
        self.logs_index = logs_index

    def _set_datetime(self, date_str):
        def _validate_datetime(dt_tuple):
            try:
                dt = {k: int(v) if v else 1 for k, v in dt_tuple._asdict().items()}
                datetime.datetime(
                    year=dt["year"],
                    month=dt["month"],
                    day=dt["day"],
                    hour=dt["hour"],
                    minute=dt["minute"],
                )
            except ValueError as e:
                self.logger.error(f"Invalid Date ({e}): {date_str}")
                raise

        date_rx = re.search(
            "(?P<year>\d\d\d\d)-?(?P<month>\d\d)?-?(?P<day>\d\d)? ?(?P<hour>\d\d)?:?(?P<minute>\d\d)?",
            date_str,
        )
        if not date_rx:
            self.logger.error(f"Invalid Date: {date_str}")
            raise ValueError(f"Invalid Date: {date_str}")

        DateTime = namedtuple("datetime", ["year", "month", "day", "hour", "minute"])
        dt_tuple = DateTime(
            year=date_rx.group("year"),
            month=date_rx.group("month"),
            day=date_rx.group("day"),
            hour=date_rx.group("hour"),
            minute=date_rx.group("minute"),
        )

        _validate_datetime(dt_tuple)
        self.dt = dt_tuple

    def _get_distinct_logs(self, day_logs_index, res_data_type):
        if self.dt.minute:
            data = {
                x: day_logs_index[x][self.dt.hour][self.dt.minute]
                for x in day_logs_index
                if self.dt.hour in day_logs_index[x]
                and self.dt.minute in day_logs_index[x][self.dt.hour]
            }
        elif self.dt.hour:
            data = {
                x: day_logs_index[x][self.dt.hour]["_count"]
                for x in day_logs_index
                if self.dt.hour in day_logs_index[x]
            }
        else:
            data = {x: day_logs_index[x]["_count"] for x in day_logs_index}

        return list(data.items()) if res_data_type == "list" else Counter(data)

    def _get_logs_per_day(self, data):
        freq_logs_cnt = self._get_distinct_logs(data["frequent_logs"], "counter")
        uniq_logs_lst = self._get_distinct_logs(data["unique_logs"], "list")

        return freq_logs_cnt, uniq_logs_lst

    def _get_logs(self, date_str):
        self._set_datetime(date_str)

        year = self.dt.year
        month = self.dt.month
        day = self.dt.day
        hour = self.dt.hour
        minute = self.dt.minute

        if (
            year
            and year not in self.logs_index
            or month
            and month not in self.logs_index[year]
            or day
            and day not in self.logs_index[year][month]
        ):
            return {}, []

        full_freq_logs_cnt = Counter()
        full_uniq_logs_lst = []

        if minute or hour or day:
            full_freq_logs_cnt, full_uniq_logs_lst = self._get_logs_per_day(
                self.logs_index[year][month][day]
            )
        elif month or year:
            month_data = [month] if month else self.logs_index[year]
            for _month in month_data:
                for _day in self.logs_index[year][_month]:
                    freq_logs_cnt, uniq_logs_lst = self._get_logs_per_day(
                        self.logs_index[year][_month][_day]
                    )
                    full_freq_logs_cnt += freq_logs_cnt
                    full_uniq_logs_lst += uniq_logs_lst

        return full_freq_logs_cnt, full_uniq_logs_lst

    def get_distinct_queries_count(self, date_str):
        freq_logs_cnt, uniq_logs_lst = self._get_logs(date_str)
        response = {"count": len(freq_logs_cnt) + len(uniq_logs_lst)}
        return response

    def get_top_popular_queries(self, date_str, size):
        size = int(size)
        freq_logs_cnt, uniq_logs_lst = self._get_logs(date_str)
        if freq_logs_cnt and uniq_logs_lst:
            freq_logs_top = freq_logs_cnt.most_common(size)
            uniq_logs_top = heapq.nlargest(size, uniq_logs_lst, key=itemgetter(1))
            top_queries = heapq.nlargest(
                size, freq_logs_top + uniq_logs_top, key=itemgetter(1)
            )
            response = {
                "queries": [
                    {"query": self.query_index[query_id], "count": query_count}
                    for query_id, query_count in top_queries
                ]
            }
            return response

        return {}
