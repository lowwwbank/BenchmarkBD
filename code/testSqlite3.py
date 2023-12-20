import sqlite3
import configparser
import time


def test():
    config = configparser.ConfigParser()
    config.read("code\\config.ini")
    result = [0] * 4
    local_db = sqlite3.connect(f"data\\{config['local']['db']}")
    cursor = local_db.cursor()
    table = config['local']['table']
    queries = [
        f"""SELECT "VendorID", COUNT(*)
                FROM "{table}" GROUP BY 1;""",
        f"""SELECT "passenger_count", AVG("total_amount")
               FROM "{table}" GROUP BY 1;""",
        f"""SELECT "passenger_count", STRFTIME('%Y', "tpep_pickup_datetime"), COUNT(*)
               FROM "{table}" GROUP BY 1, 2;""",
        f"""SELECT "passenger_count", STRFTIME('%Y', "tpep_pickup_datetime"), ROUND("trip_distance"), COUNT(*)
               FROM "{table}" GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;""",
    ]
    for i in range(4):
        for j in range(10):
            start = time.perf_counter()
            cursor.execute(queries[i])
            finish = time.perf_counter()
            result[i] += finish - start
        result[i] = result[i] / 10
    local_db.close()
    return result
