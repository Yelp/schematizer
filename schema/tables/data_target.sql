CREATE TABLE `data_target` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `target_type` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `destination` varchar(2048) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;