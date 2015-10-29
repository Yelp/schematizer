CREATE TABLE `refresh_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_identifier` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `refresh_status` int(11) NOT NULL,
  `last_refreshed_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `table_id_unique_constraint` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;