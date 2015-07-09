CREATE TABLE `consumer` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `email` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `schema_id` int(11) NOT NULL,
  `job_name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `expected_frequency` int(11) NOT NULL,
  `consumer_group_id` int(11) NOT NULL,
  `last_used_at` int(11) DEFAULT NULL,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `job_schema_unique_constraint` (`job_name`,`schema_id`),
  KEY `consumer_group_id` (`consumer_group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;