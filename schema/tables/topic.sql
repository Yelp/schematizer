CREATE TABLE `topic` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `topic` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `domain_id` int(11) NOT NULL,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `topic` (`topic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci