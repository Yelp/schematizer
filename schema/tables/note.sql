CREATE TABLE `note` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `note_type` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
  `reference_id` int(11) NOT NULL,
  `note` text COLLATE utf8_unicode_ci,
  `last_updated_by` varchar(255) COLLATE utf8_unicode_ci,
  `created_at` int(11) NOT NULL,
  `updated_at` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `note_type_reference_id_unique_constraint` (`note_type`,`reference_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;