

## event sql

```sql
CREATE TABLE `common_event` (
  `id` bigint(20) NOT NULL COMMENT '事件id, 全局唯一, 可用于幂等操作',
  `event_type` varchar(255) DEFAULT NULL COMMENT '事件类型',
  `event_binary` blob DEFAULT NULL COMMENT '事件内容',
  `created_at` timestamp(6) NOT NULL DEFAULT current_timestamp(6) COMMENT '插入时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
```

## dependencyTree

```
[info] com.today:event-bus_2.12:0.1-SNAPSHOT [S]
[info]   +-com.alibaba.otter:canal.protocol:1.0.25
[info]   +-com.github.dapeng:dapeng-core:2.0.1-SNAPSHOT
[info]   +-com.github.dapeng:dapeng-utils:2.0.1-SNAPSHOT
[info]   | +-com.github.dapeng:dapeng-core:2.0.1-SNAPSHOT
[info]   | +-io.netty:netty-all:4.1.20.Final
[info]   | 
[info]   +-com.github.wangzaixiang:scala-sql_2.12:2.0.3 [S]
[info]   | +-org.scala-lang:scala-reflect:2.12.4 [S]
[info]   | +-org.slf4j:slf4j-api:1.7.25
[info]   | +-org.slf4j:slf4j-api:1.7.9 (evicted by: 1.7.25)
[info]   | 
[info]   +-com.github.wangzaixiang:spray-json_2.12:1.3.4 [S]
[info]   +-net.virtual-void:sbt-dependency-graph:0.9.0
[info]   | +-com.dwijnand:sbt-compat:1.1.0
[info]   | 
[info]   +-org.apache.kafka:kafka-clients:1.0.0
[info]   | +-org.lz4:lz4-java:1.4
[info]   | +-org.slf4j:slf4j-api:1.7.25
[info]   | +-org.xerial.snappy:snappy-java:1.1.4
[info]   | 
[info]   +-org.slf4j:slf4j-api:1.7.13 (evicted by: 1.7.25)
[info]   +-org.slf4j:slf4j-api:1.7.25
[info]   +-org.springframework:spring-aop:4.3.5.RELEASE
[info]   | +-org.springframework:spring-beans:4.3.5.RELEASE
[info]   | | +-org.springframework:spring-core:4.3.5.RELEASE
[info]   | |   +-commons-logging:commons-logging:1.2
[info]   | |   
[info]   | +-org.springframework:spring-core:4.3.5.RELEASE
[info]   |   +-commons-logging:commons-logging:1.2
[info]   |   
[info]   +-org.springframework:spring-aspects:4.3.5.RELEASE
[info]     +-org.aspectj:aspectjweaver:1.8.9
[info]     
```