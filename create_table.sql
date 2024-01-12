CREATE TABLE stores (
    storeId VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL
);

CREATE TABLE stores_specific_products (
    storeProductId VARCHAR(255) PRIMARY KEY,
    uniqueId VARCHAR(255) NOT NULL,
    storeId VARCHAR(255) NOT NULL,
    s_p_selling_price VARCHAR(255) NOT NULL,
    s_p_availability BOOLEAN,
    s_p_size VARCHAR(255)[],
    s_p_color VARCHAR(255)[]
);

CREATE TABLE products (
    uniqueId VARCHAR(255) PRIMARY KEY,
    colorName VARCHAR(255)[],
    size VARCHAR(255)[],
    description TEXT,
    catlevel2 VARCHAR(255)[],
    productInventory INTEGER,
    newProduct BOOLEAN,
    pattern VARCHAR(255)[],
    productImage VARCHAR(255),
    color VARCHAR(255)[],
    imageUrl VARCHAR(255)[]
);

CREATE TABLE stores_specific_variants (
    storeVariantId VARCHAR(255) PRIMARY KEY,
    s_v_onSale BOOLEAN,
    s_v_displayable VARCHAR(50),
    s_v_giftCard BOOLEAN,
    storeId VARCHAR(255),
    s_v_size VARCHAR(255),
    s_v_redline BOOLEAN,
    s_v_storeAvailability VARCHAR(255)[],
    variantId VARCHAR(255),
    productId VARCHAR(255)
);

CREATE TABLE variants (
    variantId VARCHAR(255) PRIMARY KEY,
    v_currentPrice VARCHAR(255),
    v_originalPrice VARCHAR(255),
    v_displayMSRP VARCHAR(255),
    productId VARCHAR(255),
    v_color VARCHAR(255),
    v_colorCode VARCHAR(50),
    v_unbxd_color_mapping VARCHAR(255)
);