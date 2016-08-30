CREATE TABLE `avro_schema` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `avro_schema` text COLLATE utf8_unicode_ci NOT NULL,
  `topic_id` int(11) NOT NULL,
  `base_schema_id` int(11) DEFAULT NULL,
  `alias` varchar(255) DEFAULT NULL,
  `status` varchar(40) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'RW',
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `topic_id` (`topic_id`),
  KEY `alias` (`alias`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
