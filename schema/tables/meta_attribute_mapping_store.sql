CREATE TABLE `meta_attribute_mapping_store` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `entity_type` varchar(20) COLLATE utf8_unicode_ci NOT NULL,
  `entity_id` int(11) NOT NULL,
  `meta_attr_schema_id` int(11) NOT NULL,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `entity_index` (`entity_type`, `entity_id`),
  UNIQUE KEY `unique_mapping_constraint` (`entity_type`, `entity_id`, `meta_attr_schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
