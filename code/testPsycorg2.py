import time
import psycopg2
import configparser


def test():
    config = configparser.ConfigParser()
    config.read("code\\config.ini")
    db_params = config['postgres']
    table = config['local']['table']
    queries = [
        f"""SELECT "VendorID", COUNT(*)
            FROM "{table}" GROUP BY 1;""",
        f"""SELECT "passenger_count", AVG("total_amount")
           FROM "{table}" GROUP BY 1;""",
        f"""SELECT "passenger_count", EXTRACT(year FROM "tpep_pickup_datetime"), COUNT(*)
           FROM "{table}" GROUP BY 1, 2;""",
        f"""SELECT "passenger_count", EXTRACT(year FROM "tpep_pickup_datetime"), ROUND("trip_distance"), COUNT(*)
           FROM "{table}" GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;""",
    ]
    result = [0] * 4
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for i in range(4):
        for j in range(10):
            start = time.perf_counter()
            cursor.execute(queries[i])
            finish = time.perf_counter()
            result[i] += finish - start
        result[i] = result[i] / 10
    cursor.close()
    connection.close()
    return result
