from flask import Flask, jsonify, request
import psycopg2
import datetime

app = Flask(__name__)

# Connection parameters
db_params = {
    'host': 'postgres',
    'port': 5432,
    'database': 'mydatabase',
    'user': 'myuser',
    'password': 'mypassword'
}

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
    # filter_product_query = """
    #     SELECT
    #         pht.uniqueId
    #     FROM
    #         stores sh
    #     JOIN
    #         stores_specific_products sph ON sh.storeId = sph.storeId
    #     JOIN
    #         products pht ON sph.uniqueId = pht.uniqueId
    #     WHERE
    #         sh.storeId IN ('363')
    #         AND 'XS' = ANY(sph.s_p_size)
    #         AND 'Blue' = ANY(pht.color)
    #     LIMIT
    #         2060;
    # """
    filter_product_query = """
        SELECT
            pht.uniqueId,
            ARRAY_AGG(filtered_variants.variantId) AS variantIds
        FROM
            (
                SELECT
                    v.variantId,
                    v.productId
                FROM
                    stores_specific_variants sph
                JOIN
                    variants v ON sph.variantId = v.variantId
                WHERE
                    sph.storeId IN ('363', '369', '366', '2075', '361', '586')
                    AND sph.s_v_size NOT IN ('XS', 'XXL')
                    AND v.v_color NOT IN ('Blue')
            ) AS filtered_variants
        JOIN
            products pht ON filtered_variants.productId = pht.uniqueId
        GROUP BY
            pht.uniqueId
        LIMIT
            5000;
    """
    cursor.execute(filter_product_query)

    results = cursor.fetchall()
    cursor.close()
    connection.close()
    output = [{"uniqueId": row[0], "variantIds": row[1]} for row in results]
    time_clocked = datetime.datetime.now() - start
    time_taken = int(time_clocked.total_seconds() * 1000)
    response = {
        'output': output,
        'time_taken': time_taken
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