CREATE TABLE `common_event` (
  `id` bigint(20) NOT NULL AUTOINCREMENT COMMENT '自增ID, 用于锁记录',
  `unique_id` bigint(20) NOT NULL COMMENT '事件id, 全局唯一, 可用于幂等操作',
  `event_type` varchar(255) DEFAULT NULL COMMENT '事件类型',
  `event_binary` blob DEFAULT NULL COMMENT '事件内容',
  `created_at` timestamp(6) NOT NULL DEFAULT current_timestamp(6) COMMENT '插入时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8