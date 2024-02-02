CREATE INDEX idx_stores_storeId ON stores(storeId);
CREATE INDEX idx_sph_storeId ON stores_specific_products(storeId);
CREATE INDEX idx_sph_uniqueId ON stores_specific_products(uniqueId);
CREATE INDEX idx_sph_s_p_size ON stores_specific_products(s_p_size);
CREATE INDEX idx_pht_uniqueId ON products(uniqueId);
CREATE INDEX idx_pht_color ON products(color);