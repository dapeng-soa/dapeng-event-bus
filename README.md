
## 概览
- 案例
- 事件发布
    - 前置条件
    - IDL定义
    - 关键性配置
    - 发布任务服务实现
    - 事件发布
- 事件订阅
    - 依赖
    - 作为订阅者
- 既是事件发送者，也是订阅者？
- 示例项目

## 案例
- 假设一个 A 服务为事件发送方，B 服务为事件订阅方
- 假设 A 服务中的 register 接口入库操作后，会发送 RegisteredEvent
- 假设 B 服务订阅了该事件消息，由订阅者自行处理订阅到的消息

## 事件发布
### 前置条件
- 依赖项
```xml
"com.today" % "event-bus_2.12" % "0.1-SNAPSHOT"
```
- 数据库存储支持，需在业务数据库中加入此表
```SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS `dp_event_info`;
CREATE TABLE `dp_event_info` (
  `id` bigint(20) NOT NULL COMMENT '事件id，全局唯一, 可用于幂等操作',
  `event_type` varchar(255) DEFAULT NULL COMMENT '事件类型',
  `event_binary` blob COMMENT '事件内容',
  `event_topic` varchar(255) DEFAULT NULL COMMENT '事件topic',
  `event_key` varchar(255) DEFAULT NULL COMMENT '事件分区key值',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Table structure for `event_lock`
-- ----------------------------
DROP TABLE IF EXISTS `dp_event_lock`;
CREATE TABLE `dp_event_lock` (
  `id` int(11) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
--  Records of `event_lock`
-- ----------------------------
BEGIN;
INSERT INTO `dp_event_lock` VALUES ('1', 'event_lock');
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
```


### IDL定义
- 以事件双方约定的消息内容定义IDL结构体
- 规定必须为每个事件定义事件ID，以便消费者做消息幂等

`==> events.thrift`
```thrift
namespace java com.github.dapeng.user.events

/**
* 注册成功事件, 由于需要消费者做幂等,故加上事件Id
**/
struct RegisteredEvent {
    /**
    * 事件Id
    **/
    1: i64 id,
    /**
    * 用户id
    **/
    2: i64 userId
}

...more

```
#### IDL服务接口事件声明
- 接口可能会触发一个或多个事件

`== >user_service.thrift`

```thrfit
namespace java com.github.dapeng.user.service

include "user_domain.thrift"
include "events.thrift"

/**
* 事件发送端业务服务
**/
service UserService{
/**
# 用户注册
## 事件
    注册成功事件，激活事件
**/
    string register(user_domain.User user)
    (events="events.RegisteredEvent,events.ActivedEvent")
    
    ...more
    
}(group="EventTest")
```

#### IDL事件消息发布任务服务

新版本不需要在thrift里面定义定时发布消息的任务

### 关键性配置（定时任务）
`==> spring/services.xml`
注意`init-method`指定 startScheduled
```xml
<!--messageScheduled 定时发送消息bean-->
<bean id="messageTask" class="com.today.eventbus.scheduler.MsgPublishTask" init-method="startScheduled">
    <constructor-arg name="topic" value="${kafka_topic}"/>
    <constructor-arg name="kafkaHost" value="${kafka_producer_host}"/>
    <constructor-arg name="tidPrefix" value="${kafka_tid_prefix}"/>
    <constructor-arg name="dataSource" ref="tx_demo_dataSource"/>
</bean>
```
- topic kafka消息topic，领域区分(建议:领域_版本号_event)。
  如果事件发布时没有指定topic，就默认使用该topic进行消息转发。
- kafkaHost kafka集群地址(如:127.0.0.1:9091,127.0.0.1:9092)
- tidPrefix kafka事务id前缀，领域区分
- dataSource 使用业务的 dataSource

`==>config_user_service.properties`

```xml
# event config
kafka_topic=user_1.0.0_event
kafka_producer_host=127.0.0.1:9092
kafka_tid_prefix=user_1.0.0
```
在dapeng.properties中配置：
```


soa.eventbus.publish.period=500 //代表轮询数据库消息库时间，如果对消息及时性很高，请将此配置调低，建议最低为100ms，默认配置是1000ms


```

### 事件触发
- 在做事件触发前,你需要实现 `AbstractEventBus` ,并将其交由spring托管，来做自定义的本地监听分发

`==>commons/EventBus.scala`
```scala
object EventBus extends AbstractEventBus {

  /**
    * 事件在触发后，可能存在本地的监听者，以及跨领域的订阅者
    * 本地监听者可以通过实现该方法进行分发
    * 同时,也会将事件发送到其他领域的事件消息订阅者
    * @param event
    */
  override def dispatchEvent(event: Any): Unit = {
    event match {
      case e:RegisteredEvent =>
        // do somthing 
      case _ =>
        LOGGER.info(" nothing ")
    }
  }
  override def getInstance: EventBus.this.type = this
}
```
- 当本地无任何监听时==>
```scala
override def dispatchEvent(event: Any): Unit = {}
```
`==> spring/services.xml`
```xml
<bean id="eventBus" class="com.github.dapeng.service.commons.EventBus" factory-method="getInstance">
    <property name="dataSource" ref="tx_demo_dataSource"/>
</bean>
```
- 事件发布

如果没有自定义指定事件的topic和分区key，会使用`services.xml`中指定的topic和事件event_id来做消息转发。
```scala
EventBus.fireEvent(RegisteredEvent(event_id,user.id),None,None)
```
也可以自定义该事件的转发topic和用于分区的key值
```scala
EventBus.fireEvent(RegisteredEvent(event_id,user.id),Some(topic),Some(key))
```
---
## 事件定时发布修改：

在dapeng.properties加入环境变量配置
```
//每次轮询间隔事件为100ms
soa.eventbus.publish.period=100


```
在业务系统的`services.xml`中配置,指定初始化方法，即定时轮询任务的方法：
```
<bean id="messageTask" class="com.today.eventbus.scheduler.MsgPublishTask" init-method="startScheduled">
    <constructor-arg name="topic" value="${KAFKA_TOPIC}"/>
    <constructor-arg name="kafkaHost" value="${KAFKA_PRODUCER_HOST}"/>
    <constructor-arg name="tidPrefix" value="${KAFKA_TID_PREFIX}"/>
    <constructor-arg name="dataSource" ref="tx_demo_dataSource"/>
</bean>


```
### 重点： 配置轮询发布消息的时间间隔，以ms为单位，在dapeng.properties中配置
```
soa.eventbus.publish.period=500 //代表500ms


```

### 生产方因为轮询数据库发布消息，如果间隔很短，会产生大量的日志，需要修改级别，在logback下进行如下配置：


```
<!--将eventbus包下面的日志都放入单独的日志文件里 dapeng-eventbus.%d{yyyy-MM-dd}.log-->
<appender name="eventbus" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <prudent>true</prudent>
    <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
					注意： 这里detail-  后面 加自己系统的名字。 例如这里的 goods
        <fileNamePattern>${soa.base}/logs/detail-goods-eventbus.%d{yyyy-MM-dd}.log</fileNamePattern>
        <maxHistory>30</maxHistory>
    </rollingPolicy>
    <encoder>
        <pattern>%d{MM-dd HH:mm:ss SSS} %t %p - %m%n</pattern>
    </encoder>
</appender>

<!-- additivity:是否向上级(root)传递日志信息, -->
<!--com.today.eventbus包下的日志都放在上面配置的单独的日志文件里-->
<logger name="com.today.eventbus" level="DEBUG" additivity="false">
    <appender-ref ref="eventbus"/>
</logger>

<!--sql 日志显示级别-->
<logger name="druid.sql" level="OFF"/>
<logger name="wangzx.scala_commons.sql" level="DEBUG"/>
<logger name="org.apache.kafka.clients.consumer.KafkaConsumer" level="INFO"/>
<logger name="org.springframework.jdbc.datasource.DataSourceUtils" level="INFO"/>


```




---

## 事件订阅



### 依赖
```xml
<!--事件总线支持库-->
<dependency>
    <groupId>com.today</groupId>
    <artifactId>event-bus_2.12</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
<!--事件发送方api-->
<dependency>
    <groupId>com.today</groupId>
    <artifactId>user-api_2.12</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>

<!--if => sbt project--> 
"com.today" % "event-bus_2.12" % "0.1-SNAPSHOT",
"com.today" % "user-api_2.12" % "0.1-SNAPSHOT"
```
注解支持配置：

```xml
<bean id="postProcessor" class="com.today.eventbus.spring.MsgAnnotationBeanPostProcessor"/>
```
附(kafka日志级别调整)：
`==>logback.xml`


```
<logger name="org.apache.kafka.clients.consumer" level="INFO"/>
```
作为一个订阅者
```java
// java

@KafkaConsumer(groupId = "eventConsumer1", topic = "user_1.0.0_event",kafkaHostKey = "kafka.consumer.host")
public class EventConsumer {
    @KafkaListener(serializer = RegisteredEventSerializer.class)
    public void subscribeRegisteredEvent(RegisteredEvent event){
        LOGGER.info("Subscribed RegisteredEvent ==> {}",event.toString());
    }
    ...
}
```

#### 注意： 订阅方在消费消息时，处理消息可能会抛出业务异常，如果该异常导致的消息丢失不需要重试，可以增加如下配置。否则event-bus会在消费消息产生的异常时进行重试，并在重试失败后将消息发送到失败队列`xxx(serviceName)-retry-topic`,等待业务方重新消费，从而保证消息不丢失。
```
soa.msg.retry.enable=false
```
增加了上述配置后，可以自己捕获异常
```
@KafkaListener(serializer = classOf[ModifySkuBuyingPriceEventSerializer])
def modifySkuBuyingPriceEvent(event: ModifySkuBuyingPriceEvent): Unit = {
  // 重点
 try {
    logger.info(s"=====> ModifySkuBuyingPriceEvent")
    val ModifySkuBuyingPriceItemList = event.modifySkuBuyingPriceEventItems.map(
      x => build[ModifySkuBuyingPriceConsumer](x)()
    )
    val result = consumer.modifySkuBuyingPrice(ModifySkuBuyingPriceItemList) // 收到事件后调用业务接口示例
    logger.info(s"收到消息$event =>成功修改sku进价， ${result} ")
  } catch {
  //logger的写法自己定义
    case e: SoaException => logger.error("业务抛出的异常，消息不会重试", e)
  }

}
```



//scala
```
serializer = classOf[RegisteredEventSerializer]
```


#### @KafkaConsumer
- groupId 订阅者领域区分
- topic 订阅的 kafka 消息 topic 
- kafkaHostKey 可自行配置的kafka地址，默认值为`dapeng.kafka.consumer.host`。可以自定义以覆盖默认值
    - 用户只要负责把这些配置放到env或者properties里面
    - 如：`System.setProperty("kafka.consumer.host","127.0.0.1:9092");`

#### @KafkaListener
- serializer 事件消息解码器，由事件发送方提供.
## 既是消费者也是订阅者？
> 如果服务既有接口会触发事件，也存在订阅其他领域的事件情况。只要增加缺少的配置即可 



## 重点可以看如下发布者demo
```
https://github.com/leihuazhe/publish-demo
```




## 示例项目
- [事件发送端demo(sbt)](https://github.com/leihuazhe/publish-demo)
- [事件订阅端demo(sbt)](http://pms.today36524.com.cn:8083/basic-services/consumer-demo)
- [事件订阅端demo(maven)](https://github.com/StruggleYang/event-consumer)
- [eventbus](http://pms.today36524.com.cn:8083/basic-services/eventBus)






---


[原地址](http://pms.today36524.com.cn:8083/basic-services/eventBus
)
