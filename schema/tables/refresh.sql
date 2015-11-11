CREATE TABLE `refresh` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `source_id` int(11) NOT NULL,
  `status` int(11) NOT NULL,
  `offset` int(11) NOT NULL,
  `batch_size` int(11) NOT NULL,
  `priority` int(11) NOT NULL,
  `filter_condition` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `source_id` (`source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;