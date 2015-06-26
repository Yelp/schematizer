CREATE TABLE `avro_schema_element` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `avro_schema_id` int(11) NOT NULL,
  `key` varchar(512) NOT NULL,
  `element_type` varchar(64) NOT NULL,
  `doc` text NOT NULL,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `avro_schema_id` (`avro_schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
