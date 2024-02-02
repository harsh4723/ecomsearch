from flask import Flask, jsonify, request
import psycopg2
import datetime
import redis
import sys
import aioredis
import asyncio
import json
import aerospike
import random


app = Flask(__name__)

# Connection parameters
db_params = {
    'host': '35.245.28.175',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword'
}

store_ids = ['939', '154', '768', '215', '42', '761', '499', '134', '340', '812', '813', '817', '678', '713', '121', '122', '59', '58', '414', '259', '411', '412', '296', '370', '2064', '2065', '290', '291', '318', '594', '706', '700', '316', '270', '274', '110', '394', '83', '80', '798', '309', '311', '100', '101', '902', '903', '783', '459', '620', '573', '606', '68', '250', '886', '1632', '656', '654', '735', '980', '568', '755', '466', '226', '161', '163', '967', '969', '396', '150', '153', '880', '558', '555', '112', '235', '958', '955', '323', '2033', '899', '611', '75', '73', '483', '353', '801', '2009', '217', '762', '662', '661', '493', '693', '698', '289', '288', '938', '2017', '715', '712', '120', '265', '777', '57', '929', '2061', '922', '985', '86', '85', '827', '1521', '522', '366', '362', '363', '2075', '361', '586', '587', '300', '301', '380', '384', '102', '107', '901', '628', '64', '66', '818', '972', '657', '655', '184', '739', '502', '468', '637', '465', '563', '564', '618', '905', '865', '864', '2027', '156', '159', '238', '959', '875', '2037', '320', '892', '776', '207', '148', '71', '687', '945', '942', '940', '2001', '688', '2008', '680', '298', '2909', '732', '368', '787', '253', '490', '285', '936', '2012', '128', '537', '536', '981', '520', '249', '623', '979', '504', '567', '552', '878', '897', '2007', '359', '267', '292', '144', '314', '390', '525', '109', '242', '105', '784', '375', '515', '332', '971', '106', '180', '187', '507', '500', '166', '162', '964', '114', '509', '125', '489', '437', '800', '681', '658', '665', '808', '179', '352', '258', '147', '723', '119', '2010', '200', '126', '664', '138', '1471', '1446', '2019', '402', '401', '2013', '344', '336', '676', '369', '123', '129', '55', '424', '416', '2066', '988', '195', '194', '523', '2073', '914', '2076', '531', '580', '2023', '305', '900', '2045', '904', '925', '645', '510', '512', '430', '621', '559', '2050', '406', '185', '546', '221', '986', '759', '99', '98', '91', '225', '160', '963', '529', '724', '2020', '2103', '2101', '501', '2029', '605', '601', '603', '602', '230', '49', '876', '954', '327', '324', '895', '140', '610', '76', '648', '484', '753', '358', '443', '498', '2074', '505', '1671', '745', '438', '934', '458', '63', '257', '660', '348', '2912', '2907', '2911', '651', '860', '2906', '685', '2910', '2913', '2916', '2903', '2914', '2904']

def generate_random_query():
    random_store_ids = random.sample(store_ids, 6)

    filter_product_query = f"""
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
            EXISTS (SELECT 1 FROM unnest(ARRAY{random_store_ids}) AS x(storeId) WHERE x.storeId = sh.storeId)
            AND '{random.choice(['XS', 'S', 'M', 'L', 'XL'])}' != ANY(sph.s_p_size)
            AND '{random.choice(['Red', 'Blue', 'Green', 'Black', 'White'])}' != ANY(pht.color)
        GROUP BY
            pht.uniqueId
        LIMIT
            5000;
    """

    return filter_product_query


@app.route("/filter", methods=["POST"])
def retrieve_for_query():
    start = datetime.datetime.now()
    data = request.get_json()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    filter_product_query = generate_random_query()
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


