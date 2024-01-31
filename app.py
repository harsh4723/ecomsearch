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

redis_host = "redis" 
redis_port = 6379

redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

config = {
    'hosts': [('as1', 3000)]
}

asclient = aerospike.client(config).connect()


@app.route('/v2/stores/insertbatch', methods=['POST'])
def insert_batch_v2():
    ## redis insert 
    data = request.get_json()
    print("Got reuqest for inserting ", len(data))
    for json_obj in data:
        values = {
            "storeId": str(json_obj["storeId"]),
            "name": str(json_obj["name"]),
            "location":  str(json_obj["location"])
        }
        store_key = "store:"+ json_obj["storeId"]
        for products in json_obj["products"]:
            products["storeProductId"] = json_obj["storeId"]+ "_" + products["uniqueId"]
            products["storeId"] = json_obj["storeId"]
            product_values = {
                "storeProductId": str(products["storeProductId"]),
                "uniqueId": str(products["uniqueId"]),
                "storeId":  str(products["storeId"]),
                "s_p_selling_price": str(products["s_p_selling_price"]),
                "s_p_availability" : str(products["s_p_availability"]),
                "s_p_size": str(products["s_p_color"])
            }
            store_products_key = "store_products:"+ products["storeProductId"]
            
            for variants in products.get("variants"):
                variants["productId"] = products["uniqueId"]
                variants["storeId"] = json_obj["storeId"]
                variants["storeVariantId"] = json_obj["storeId"]+ "_" + variants["variantId"]
                variants_values = {
                    "storeVariantId": str(variants.get("storeVariantId")),
                    "productId": str(variants.get("productId")),
                    "storeId": str(variants.get("storeId")),
                    "variantId": str(variants.get("variantId")),
                    "s_v_onSale": str(variants.get("s_v_onSale")),
                    "s_v_displayable": str(variants.get("s_v_displayable")),
                    "s_v_giftCard": str(variants.get("s_v_giftCard")),
                    "s_v_size": str(variants.get("s_v_size")),
                    "s_v_redline": str(variants.get("s_v_redline")),
                    "s_v_storeAvailability": str(variants.get("s_v_storeAvailability"))
                }
                hash_key_variants = "store_variants:"+ variants.get("storeVariantId")
                redis_client.hmset(hash_key_variants, variants_values)
            
            redis_client.hmset(store_products_key, product_values)
        
        redis_client.hmset(store_key,values)
    
    return jsonify({'message': 'Batch insertion for stores successful'}), 201


@app.route("/v2/products/insertbatch", methods=["POST"])
def insert_products_v2():
    data = request.get_json()
    for json_obj in data:
        values = {
            "colorName": str(json_obj.get("colorName")),
            "size": str(json_obj.get("size")),
            "description": str(json_obj.get("description")),
            "uniqueId": str(json_obj.get("uniqueId")),
            "catlevel2": str(json_obj.get("catlevel2")),
            "productInventory": str(json_obj.get("productInventory")),
            "newProduct": str(json_obj.get("newProduct")),
            "pattern": str(json_obj.get("pattern")),
            "productImage": str(json_obj.get("productImage")),
            "color": str(json_obj.get("color")),
            "imageUrl": str(json_obj.get("imageUrl"))
        }
        hash_key_products = "products:"+ json_obj.get("uniqueId")
        for varinats in json_obj.get("variants"):
            varinats["productId"] = json_obj.get("uniqueId")
            variants_values = {
                "variantId": str(varinats.get("variantId")),
                "v_currentPrice": str(varinats.get("v_currentPrice")),
                "v_originalPrice": str(varinats.get("v_originalPrice")),
                "v_displayMSRP": str(varinats.get("v_displayMSRP")),
                "productId": str(varinats.get("productId")),
                "v_color": str(varinats.get("v_color")),
                "v_colorCode": str(varinats.get("v_colorCode")),
                "v_unbxd_color_mapping": str(varinats.get("v_unbxd_color_mapping"))
            }
            hash_key_variants = "variant:"+ varinats.get("variantId")
            redis_client.hmset(hash_key_variants, variants_values)

        redis_client.hmset(hash_key_products, values)
    
    return jsonify({'message': 'Batch insertion for products successful'}), 201


lua_script = """
local response = {}
for i, productId in ipairs(KEYS) do
    local productKey = "products:" .. productId
    local _product = redis.call("HGETALL", productKey)

    local stores_vals = {}
    for j, storeId in ipairs(ARGV) do
        local storeProductKey = "store_products:" .. storeId .. "_" .. productId
        local storeKey = "store:" .. storeId
        local store_products = redis.call("HGETALL", storeProductKey)
        local _store = redis.call("HGETALL", storeKey)

        local store_info = {}
        for k, v in pairs(_store) do
            store_info[k] = v
        end
        for k, v in pairs(store_products) do
            store_info[k] = v
        end

        table.insert(stores_vals, store_info)
    end

    _product["stores"] = stores_vals
    table.insert(response, _product)
end

return cjson.encode(response)
"""

@app.route('/v1/product/details',  methods= ['POST'])
def get_products_details_v1():
    start = datetime.datetime.now()
    data = request.get_json()
    response = []
    for product in data["products"]:
        _product = redis_client.hgetall("products:"+ product["uniqueId"])
        stores_vals = []
        for store in product["stores"]:
            store_products = redis_client.hgetall("store_products:"+store["id"]+"_"+product["uniqueId"])
            _store = redis_client.hgetall("store:"+store["id"])
            stores_vals.append({**_store, **store_products})
        _product["stores"] = stores_vals
        response.append(_product)
    
    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    res = {
        "products": response,
        "msTaken" : time_taken,
        'numProducts' : len(response)
    }
    return jsonify(res), 200

'''
 for k, v in pairs(_store) do
    store_info[k] = v
end
for k, v in pairs(store_products) do
    store_info[k] = v
end
'''

@app.route('/v2/product/details', methods= ['POST'])
def get_products_details_v2():
    start = datetime.datetime.now()
    data = request.get_json()

    lua_script = """
    local response = {}
    local products_json = ARGV[1]

    local products = cjson.decode(products_json)

    for _, product in ipairs(products["products"]) do
        local _product = redis.call('HGETALL', "products:" .. product["uniqueId"])
        local product_json = {}
        for i = 1, #_product, 2 do
            local field = _product[i]
            local value = _product[i+1]
            product_json[field] = value
        end
        
        local stores_vals = {}

        for _, store in ipairs(product["stores"]) do
            local store_products = redis.call('HGETALL', "store_products:" .. store["id"] .. "_" .. product["uniqueId"])
            local _store = redis.call('HGETALL', "store:" .. store["id"])
            local store_info = {}
            for i = 1, #_store, 2 do
                local field = _store[i]
                local value = _store[i + 1]
                store_info[field] = value
            end

            for i = 1, #store_products, 2 do
                local field = store_products[i]
                local value = store_products[i + 1]
                store_info[field] = value
            end
            
            
            table.insert(stores_vals, store_info)
        end

        product_json["stores"] = stores_vals
        table.insert(response, product_json)
    end

    return cjson.encode(response)
    """
    data_json = json.dumps(data)

    result = redis_client.eval(lua_script, 0, data_json)

    # Decode the result from JSON to Python
    response = json.loads(result)
    #Convert Python data to Lua-like table
    # lua_data = []
    # for product in data["products"]:
    #     lua_product = {
    #         "uniqueId": product["uniqueId"],
    #         "stores": [{"id": store["id"]} for store in product["stores"]]
    #     }
    #     lua_data.append(lua_product)

    # Execute the Lua script
    #result = redis_client.execute_command('EVAL', lua_script, 0, *lua_data)
    # product_ids = [product["uniqueId"] for product in data["products"]]
    # store_ids = [store["id"] for product in data["products"] for store in product["stores"]]

    #result = redis_client.eval(lua_script, len(lua_product), *lua_data)
    #response = json.loads(result)

    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    res = {
        "products": response,
        "msTaken" : time_taken,
        'numProducts' : len(response)
    }
    return jsonify(res), 200

@app.route('/v5/product/details', methods= ['POST'])
def get_products_details_v5():
    start = datetime.datetime.now()
    data = request.get_json()
    response = []
    for product in data["products"]:
        _product1 = redis_client.get("products:"+ product["uniqueId"])
        _product = {}
        if _product1 is not None:
            _product = json.loads(_product1)
        stores_vals = []
        for store in product["stores"]:
            store_products1 = redis_client.get("store_products:"+store["id"]+"_"+product["uniqueId"])
            store_products = {}
            if store_products1 is not None: 
                store_products = json.loads(store_products1)
            _store1 = redis_client.get("store:"+store["id"])
            _store = {}
            if _store1 is not None:
                _store = json.loads(_store1)
            stores_vals.append({**_store, **store_products})
        _product["stores"] = stores_vals
        response.append(_product)
    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    res = {
        "products": response,
        "msTaken" : time_taken,
        'numProducts' : len(response)
    }
    return jsonify(res), 200

@app.route('/v6/product/details', methods= ['POST'])
def get_products_details_v6():
    start = datetime.datetime.now()
    data = request.get_json()
    response = []

    # Prepare the list of keys to fetch using MGET for products

    product_keys = []
    store_product_keys = []
    unique_store_keys = {}
    for product in data["products"]:
        product_keys.append("products:" + product["uniqueId"])
        for store in product["stores"]:
            store_product_keys.append("store_products:"+store["id"]+"_"+product["uniqueId"])
            unique_store_keys["store:"+store["id"]] = 1
    
    product_vals = redis_client.mget(product_keys)
    store_product_vals =  redis_client.mget(store_product_keys)
    store_vals = redis_client.mget(list(unique_store_keys.keys()))
    
    product_val_map = {}
    for product in product_vals:
        if product is not None:
            _product = json.loads(product)
            product_val_map["products:"+_product["uniqueId"]] = _product
    # print("harsh prrrrrr", product_val_map)
    store_vals_map = {}
    for store in store_vals:
        if store is not None:
            _store = json.loads(store)
            store_vals_map["store:"+_store["storeId"]] = _store

    store_prod_vals_map = {}
    for store_prod in store_product_vals:
        if store_prod is not None:
            _store_prod = json.loads(store_prod)
            store_prod_vals_map["store_products:"+_store_prod["storeProductId"]] = _store_prod

    for product in data["products"]:
        # print("uniqueId", product["uniqueId"])
        # sys.stdout.flush()
        _product = product_val_map.get("products:"+ product["uniqueId"],{})
        # print("Harsh prrrrr",_product)
        # sys.stdout.flush()
        stores_vals = []
        for store in product["stores"]:
            store_products = store_prod_vals_map.get("store_products:"+store["id"]+"_"+product["uniqueId"], {})
            
            _store = store_vals_map.get("store:"+store["id"],{})
            stores_vals.append({**_store, **store_products})
        _product["stores"] = stores_vals
        response.append(_product)
    
    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    res = {
        "products": response,
        "msTaken" : time_taken,
        'numProducts' : len(response)
    }
    return jsonify(res), 200


@app.route('/v3/product/details', methods=['POST'])
def get_products_details_v3():
    start = datetime.datetime.now()
    data = request.get_json()
    response = []

    # Create a pipeline for batched Redis commands
    pipeline = redis_client.pipeline()

    # Queue up all hgetall commands for products
    for product in data["products"]:
        pipeline.hgetall("products:" + product["uniqueId"])

    # Execute the pipeline and get results for products
    product_results = pipeline.execute()

    for i, product in enumerate(data["products"]):
        _product = product_results[i]
        stores_vals = []

        # Queue up hgetall commands for store products and stores
        for store in product["stores"]:
            pipeline.hgetall("store_products:" + store["id"] + "_" + product["uniqueId"])
            pipeline.hgetall("store:" + store["id"])

        # Execute the pipeline and get results for store products and stores
        store_results = pipeline.execute()

        # Process the results and build the response
        for j, store in enumerate(product["stores"]):
            store_products = store_results[j * 2]
            _store = store_results[j * 2 + 1]
            stores_vals.append({**_store, **store_products})

        _product["stores"] = stores_vals
        response.append(_product)

    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    res = {
        "products": response,
        "msTaken": time_taken,
        'numProducts': len(response)
    }
    return jsonify(res), 200



@app.route('/stores/insertbatch', methods=['POST'])
def insert_stores():
    data = request.get_json()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_stores_query = """
        INSERT INTO stores (
            storeId, name, location
        ) VALUES (
            %s, %s, %s
        ) 
        ON CONFLICT (storeId) DO UPDATE SET
        name = EXCLUDED.name,
        location = EXCLUDED.location;
    """

    insert_store_specific_products_query = """
        INSERT INTO stores_specific_products (
            storeProductId, uniqueId, storeId, s_p_selling_price, s_p_availability, 
            s_p_size, s_p_color
        ) VALUES(
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (storeProductId) DO UPDATE SET
        uniqueId = EXCLUDED.uniqueId,
        storeId = EXCLUDED.storeId,
        s_p_selling_price = EXCLUDED.s_p_selling_price,
        s_p_availability = EXCLUDED.s_p_availability,
        s_p_size = EXCLUDED.s_p_size,
        s_p_color = EXCLUDED.s_p_color;
    """
    insert_store_specific_varaints_query = """
        INSERT INTO stores_specific_variants (
            storeVariantId, productId, storeId, variantId, s_v_onSale, 
            s_v_displayable, s_v_giftCard, s_v_size, s_v_redline, s_v_storeAvailability
        ) VALUES(
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (storeVariantId) DO UPDATE SET
        productId = EXCLUDED.productId,
        storeId = EXCLUDED.storeId,
        variantId = EXCLUDED.variantId,
        s_v_onSale = EXCLUDED.s_v_onSale,
        s_v_displayable = EXCLUDED.s_v_displayable,
        s_v_giftCard = EXCLUDED.s_v_giftCard,
        s_v_size = EXCLUDED.s_v_size,
        s_v_redline = EXCLUDED.s_v_redline,
        s_v_storeAvailability = EXCLUDED.s_v_storeAvailability;
    """
    for json_obj in data:
        
        values = (json_obj["storeId"], json_obj["name"], json_obj["location"])
        for products in json_obj["products"]:
            products["storeProductId"] = json_obj["storeId"]+ "_" + products["uniqueId"]
            products["storeId"] = json_obj["storeId"]
            
            product_values = (products["storeProductId"], products["uniqueId"], products["storeId"],products["s_p_selling_price"],products["s_p_availability"],
                              products["s_p_size"], products["s_p_color"])
            
            for variants in products["variants"]:
                variants["productId"] = products["uniqueId"]
                variants["storeId"] = json_obj["storeId"]
                variants["storeVariantId"] = json_obj["storeId"]+ "_" + variants["variantId"]
                variants_values = (variants.get("storeVariantId"), variants.get("productId"), variants.get("storeId"), variants.get("variantId"),
                                   variants.get("s_v_onSale"), variants.get("s_v_displayable"), variants.get("s_v_giftCard"), variants.get("s_v_size"),
                                   variants.get("s_v_redline"),variants.get("s_v_storeAvailability"))
                cursor.execute(insert_store_specific_varaints_query, variants_values)
            
            cursor.execute(insert_store_specific_products_query, product_values)
        
        cursor.execute(insert_stores_query, values)
    
    connection.commit()

    cursor.close()
    connection.close()

    return jsonify({'message': 'Batch insertion for stores successful'}), 201

@app.route("/products/insertbatch", methods=["POST"])
def insert_products():
    data = request.get_json()
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    insert_products_query = """
        INSERT INTO products (
        colorName, size, description, uniqueId, catlevel2,
        productInventory, newProduct, pattern, productImage,
        color, imageUrl
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (uniqueId) DO UPDATE SET
            colorName = EXCLUDED.colorName,
            size = EXCLUDED.size,
            description = EXCLUDED.description,
            catlevel2 = EXCLUDED.catlevel2,
            productInventory = EXCLUDED.productInventory,
            newProduct = EXCLUDED.newProduct,
            pattern = EXCLUDED.pattern,
            productImage = EXCLUDED.productImage,
            color = EXCLUDED.color,
            imageUrl = EXCLUDED.imageUrl;
    """
    insert_variants_query = """
        INSERT INTO variants (
        variantId, v_currentPrice, v_originalPrice, v_displayMSRP, productId,
        v_color, v_colorCode, v_unbxd_color_mapping
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (variantId) DO UPDATE SET
            v_currentPrice = EXCLUDED.v_currentPrice,
            v_originalPrice = EXCLUDED.v_originalPrice,
            v_displayMSRP = EXCLUDED.v_displayMSRP,
            productId = EXCLUDED.productId,
            v_color = EXCLUDED.v_color,
            v_colorCode = EXCLUDED.v_colorCode,
            v_unbxd_color_mapping = EXCLUDED.v_unbxd_color_mapping;
    """

    for json_obj in data:
        values = (
            json_obj.get("colorName"),
            json_obj.get("size"),
            json_obj.get("description"),
            json_obj.get("uniqueId"),
            json_obj.get("catlevel2"),
            json_obj.get("productInventory"),
            json_obj.get("newProduct"),
            json_obj.get("pattern"),
            json_obj.get("productImage"),
            json_obj.get("color"),
            json_obj.get("imageUrl")
        )
        for varinats in json_obj.get("variants"):
            varinats["productId"] = json_obj.get("uniqueId")
            varinats_values = (varinats.get("variantId"), varinats.get("v_currentPrice"), varinats.get("v_originalPrice"), varinats.get("v_displayMSRP"),varinats.get("productId"),
                               varinats.get("v_color"),varinats.get("v_colorCode"),varinats.get("v_unbxd_color_mapping"))
            cursor.execute(insert_variants_query, varinats_values)

        cursor.execute(insert_products_query, values)
    
    connection.commit()

    cursor.close()
    connection.close()

    return jsonify({'message': 'Batch insertion for products successful'}), 201



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
    
    return jsonify(response)

@app.route('/')
def get_postgres_version():
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_params)

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Example: Execute a simple query
    cursor.execute("SELECT version();")
    version = cursor.fetchone()

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return jsonify({'PostgreSQL version': version[0]})



########### aerospike ########

@app.route('/v3/stores/insertbatch', methods=['POST'])
def insert_stores_v3():
    data = request.get_json()
    # Specify the serializer (SERIALIZER_PYTHON in this example)
    policy = {'serializer': aerospike.SERIALIZER_JSON}
    for json_obj in data:
        store_key = ('test', 'stores', str(json_obj["storeId"]))
        store_bins = {
            'name': json_obj["name"],
            'location': json_obj["location"]
        }

        asclient.put(store_key, store_bins, policy=policy)

        for products in json_obj["products"]:
            product_key = ('test', 'store_specific_products', str(json_obj["storeId"]) + "_" + str(products["uniqueId"]))
            product_bins = {
                'uniqueId': str(products["uniqueId"]),
                'storeId': str(json_obj["storeId"]),
                's_p_sell': str(products["s_p_selling_price"]),
                's_p_avail': str(products["s_p_availability"]),
                's_p_size': str(products["s_p_size"]),
                's_p_color': str(products["s_p_color"])
            }

            asclient.put(product_key, product_bins, policy=policy)

            for variants in products["variants"]:
                variant_key = ('test', 'store_specific_variants', str(json_obj["storeId"]) + "_" + str(variants["variantId"]))
                variant_bins = {
                    'productId': str(products.get("uniqueId")),
                    'storeId': str(json_obj.get("storeId")),
                    'variantId': str(variants.get("variantId")),
                    's_v_onSale': str(variants.get("s_v_onSale")),
                    's_v_displ': str(variants.get("s_v_displayable")),
                    's_v_giftCa': str(variants.get("s_v_giftCard")),
                    's_v_size': str(variants.get("s_v_size")),
                    's_v_redline': str(variants.get("s_v_redline")),
                    's_v_storeAv': str(variants.get("s_v_storeAvailability"))
                }

                asclient.put(variant_key, variant_bins, policy=policy)

    return jsonify({'message': 'Batch insertion for stores successful'}), 201


@app.route("/v3/products/insertbatch", methods=["POST"])
def insert_products_v3():
    data = request.get_json()
    for json_obj in data:
        product_key = ("test", "products", str(json_obj.get("uniqueId")))
        product_bins = {
            "colorName": str(json_obj.get("colorName")),
            "size": str(json_obj.get("size")),
            "description": str(json_obj.get("description")),
            "uniqueId": str(json_obj.get("uniqueId")),
            "catlevel2": str(json_obj.get("catlevel2")),
            "productInve": str(json_obj.get("productInventory")),
            "newProduct": str(json_obj.get("newProduct")),
            "pattern": str(json_obj.get("pattern")),
            "productIme": str(json_obj.get("productImage")),
            "color": str(json_obj.get("color")),
            "imageUrl": str(json_obj.get("imageUrl"))
        }
        asclient.put(product_key, product_bins)
        for varinats in json_obj.get("variants"):
            variant_key = ("test", "variants", str(varinats.get("variantId")))
            varinats["productId"] = json_obj.get("uniqueId")
            variant_bins = {
                "variantId": str(varinats.get("variantId")),
                "v_currentP": str(varinats.get("v_currentPrice")),
                "v_origina": str(varinats.get("v_originalPrice")),
                "v_displ": str(varinats.get("v_displayMSRP")),
                "productId": str(varinats.get("productId")),
                "v_color": str(varinats.get("v_color")),
                "v_colorCode": str(varinats.get("v_colorCode")),
                "v_unbxd_co": str(varinats.get("v_unbxd_color_mapping"))
            }
            asclient.put(variant_key, variant_bins)

    return jsonify({'message': 'Batch insertion for products successful'}), 201


@app.route('/v7/product/details', methods=['POST'])
def get_products_details_v7():
    start = datetime.datetime.now()
    data = request.get_json()
    response = []

    for product in data["products"]:
        unique_id = product["uniqueId"]

        # Fetch product details from Aerospike
        aerospike_key_product = ('test', 'products', unique_id)
        _, _, product_record = asclient.get(aerospike_key_product)
        if product_record:
            stores_vals = []

            for store in product["stores"]:
                store_id = store["id"]

                # Fetch store details from Aerospike
                aerospike_key_store = ('test', 'stores', store_id)
                _, _, store_record = asclient.get(aerospike_key_store)

                # Fetch store products from Aerospike
                aerospike_key_store_product = ('test', 'store_specific_products', f'{store_id}_{unique_id}')
                store_product_record ={}
                try:
                    _, _, store_product_record = asclient.get(aerospike_key_store_product)
                except:
                    pass

                # Combine store details and store products
                stores_vals.append({**store_record, **store_product_record})

            # Combine product details and stores
            response.append({**product_record, "stores": stores_vals})

    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    res = {
        "products": response,
        "msTaken": time_taken,
        'numProducts': len(response)
    }

    return jsonify(res), 200

def batch_fetch_records(keys):
    try:
        records = asclient.batch_read(keys)
        return records
    except aerospike.exception.AerospikeError as e:
        print(f"Error fetching records: {e}")
        return {}


@app.route('/v8/product/details', methods=['POST'])
def get_products_details_v8():
    start = datetime.datetime.now()
    data = request.get_json()
    response = []
    product_keys = []
    store_keys = []
    store_product_keys = []
    unique_store_keys = {}
  
    for product in data["products"]:
        unique_id = product["uniqueId"]

        aerospike_key_product = ('test', 'products', unique_id)
        product_keys.append(aerospike_key_product)

        for store in product["stores"]:
            store_id = store["id"]

            aerospike_key_store = ('test', 'stores', store_id)
            unique_store_keys[aerospike_key_store] = 1

            aerospike_key_store_product = ('test', 'store_specific_products', f'{store_id}_{unique_id}')
            store_product_keys.append(aerospike_key_store_product)

    # Batch fetching product records
    product_records = batch_fetch_records(product_keys)
    store_records = batch_fetch_records(unique_store_keys.keys())
    store_product_records = batch_fetch_records(store_product_keys)
    
    product_val_map = {}
    for br in product_records.batch_records:
        product_val_map[br.record[0][2]] = br.record[-1]
    
    # print("dddddd", product_val_map)
    # sys.stdout.flush()
    store_vals_map = {}
    for br in store_records.batch_records:
        store_vals_map[br.record[0][2]] = br.record[-1]

    store_prod_vals_map = {}
    for br in store_product_records.batch_records:
        try:
            store_prod_vals_map[br.record[0][2]] = br.record[-1]
        except:
            pass

    for product in data["products"]:
        _product = product_val_map.get(product["uniqueId"],{})
        stores_vals = []
        for store in product["stores"]:
            store_products = store_prod_vals_map.get(store["id"]+"_"+product["uniqueId"], {})
            
            _store = store_vals_map.get(store["id"],{})
            stores_vals.append({**_store, **store_products})
        _product["stores"] = stores_vals
        response.append(_product)


    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    res = {
        "products": response,
        "msTaken": time_taken,
        'numProducts': len(response)
    }

    return jsonify(res), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)



# SELECT
#     pht.uniqueId,
#     v.variantId
# FROM
#     stores sh
# JOIN
#     stores_specific_variants sph ON sh.storeId = sph.storeId
# LEFT JOIN
#     variants v ON sph.variantId = v.variantId
# JOIN
#     products pht ON v.productId = pht.uniqueId
# WHERE
#     sh.storeId IN ('363')
#     AND sph.s_v_size NOT IN ('XS', 'XXL')
#     AND v.v_color IN ('Blue', 'Brown')
# 	AND 'Reddish Brown' = ANY(pht.colorName)
# GROUP BY
#     pht.uniqueId,
# 	v.variantId
# LIMIT
#     2000;
    

