from flask import Flask, jsonify, request
import psycopg2
import datetime
import redis
import sys
import aioredis
import asyncio
import json
import aerospike


app = Flask(__name__)

# Connection parameters
db_params = {
    'host': '35.245.28.175',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword'
}

@app.route("/filter", methods=["POST"])
def retrieve_for_query():
    start = datetime.datetime.now()
    data = request.get_json()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    filter_product_query = """
        SELECT
            pht.uniqueId,
            ARRAY_AGG( sh.storeId) AS stores
        FROM
            stores sh
        JOIN
            stores_specific_products sph ON sh.storeId = sph.storeId
        JOIN
            products pht ON sph.uniqueId = pht.uniqueId
        WHERE
            EXISTS (SELECT 1 FROM unnest(ARRAY['363', '369', '366', '2075', '361', '586']) AS x(storeId) WHERE x.storeId = sh.storeId)
            AND 'XS' != ANY(sph.s_p_size)
            AND 'Brown' != ANY(pht.color)
        GROUP BY
            pht.uniqueId
        LIMIT
            5000;
    """
    cursor.execute(filter_product_query)

    results = cursor.fetchall()
    cursor.close()
    connection.close()
    output = []
    for row in results:
        new_store_ids = []
        for store_id in row[1]:
            new_store_ids.append({"id":store_id})
        output.append({"uniqueId": row[0], "stores":new_store_ids})

    #output = [{"uniqueId": row[0], "stores": row[1]} for row in results]
    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    response = {
        'products': output,
        'time_taken': time_taken,
        'numProducts': len(output)
    }
    print("Harsh len of products",len(output))
    sys.stdout.flush()
    print("Harsh mstaken",time_taken)
    sys.stdout.flush()
    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)


