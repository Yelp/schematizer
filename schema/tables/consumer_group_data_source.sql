CREATE TABLE `consumer_group_data_source` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `consumer_group_id` int(11) NOT NULL,
  `data_source_type` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `data_source_id` int(11) NOT NULL,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;