import psycopg2
import datetime
import json
import aerospike
import sys

db_params = {
    'host': '35.245.28.175',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword'
}

config = {
    'hosts': [('35.194.91.164', 3000)]
}

asclient = aerospike.client(config).connect()

f = open('express_products4.json')
data = json.load(f)

concated_data = data

stores = {}

variants = []

product_key_map = {}

for x in concated_data:
    product_key_map[x["uniqueId"]] = x
    for y in x["variants"]:
        for z in y["v_storeIds"]:
            new_dict = {"s_"+key: value for key, value in y.items() if key in ["v_displayable","v_size", "v_redline", "v_giftCard","v_onSale","v_storeAvailability"]}
            new_dict["variantId"] = y["variantId"]
            new_dict["productId"] = x["uniqueId"]
            #new_dict2 = {"uniqueId":x["uniqueId"],"variants":[new_dict] }
            if z in stores:
                stores[z]["variants"].append(new_dict)
            else:
                stores[z]={"storeId":z,"name":"A2Z store","location":"blr","variants":[new_dict]}

for k,v in stores.items():
    pro = {}
    for x in stores[k]["variants"]:
        if x["productId"] in pro:
            pro[x["productId"]].append(x)
        else:
            pro[x["productId"]] = [x]
    for k1, v1 in pro.items():
        if "products" in stores[k]:
            stores[k]["products"].append({"uniqueId":k1,"s_p_selling_price":product_key_map[k1]["selling_price"],"s_p_availability":product_key_map[k1]["availability"],"s_p_size":product_key_map[k1]["size"],"s_p_color":product_key_map[k1]["color"], "variants":v1})# add "variants":v1 to include variants
        else:
            stores[k]["products"]= [{"uniqueId":k1,"s_p_selling_price":product_key_map[k1]["selling_price"],"s_p_availability":product_key_map[k1]["availability"],"s_p_size":product_key_map[k1]["size"],"s_p_color":product_key_map[k1]["color"], "variants":v1}] # add "variants":v1 to include variants
    del stores[k]["variants"]



products = []
for x in concated_data:
    product_dict = {key: value for key, value in x.items() if key in ["uniqueId","description","pattern","size","catlevel2","productImage","imageUrl","newProduct","productInventory","color","colorName"]}
    variants_arr = []
    for y in x["variants"]:
        new_dict = {key: value for key, value in y.items() if key in ["v_color","v_colorCode", "v_currentPrice","v_originalPrice","v_displayMSRP","v_unbxd_color_mapping","variantId"]}
        variants_arr.append(new_dict)
    product_dict["variants"] = variants_arr
    products.append(product_dict)


list_stores = list(stores.values())
print(len(products))
print(len(list_stores))



def insert_stores():
    data = list_stores
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

    print("Added stores")


def insert_products():
    data = products
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

    print("Added products")


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
    print("postgres version", version[0])


def retrieve_for_query():
    start = datetime.datetime.now()
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
    print("Len of products",len(output))
    #print("Response", response)
    print("time taken", time_taken)


## aerospike
def insert_stores_v3():
    data = list_stores
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

    print("Added stores")


def insert_products_v3():
    data = products
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

    print("Added products")


def batch_fetch_records(keys):
    try:
        records = asclient.batch_read(keys)
        return records
    except aerospike.exception.AerospikeError as e:
        print(f"Error fetching records: {e}")
        return {}
aerodata = {
    "numProducts": 158,
    "products": [
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "00305330"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "00305332"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01012251"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01012412C"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01203182F"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01698274"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01698892"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01760321C"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01760500C"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "01761043C"
        },
        {
            "stores": [
                {
                    "id": "369"
                }
            ],
            "uniqueId": "09807079"
        }
    ],
    "time_taken": 177
}

def get_products_details_v8():
    start = datetime.datetime.now()
    data = aerodata
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
    
    print("Harsh product_records",product_records)
    sys.stdout.flush()
    
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
    print("Harsh products",response)
    sys.stdout.flush()
    print("Harsh len of products",len(response))
    sys.stdout.flush()
    print("Harsh mstaken",time_taken)
    sys.stdout.flush()

#get_postgres_version()

# insert_stores()
# insert_products()
#insert_stores_v3()
#insert_products_v3()
get_products_details_v8()