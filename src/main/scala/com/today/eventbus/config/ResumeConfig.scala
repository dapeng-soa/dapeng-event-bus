package com.today.eventbus.config


import java.util

import com.today.eventbus.utils.CommonUtil
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.collection.immutable

/**
  *
  * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
  * @since 2018-12-05 10:53 AM
  */
case class ResumeConfig(groupId: String, topic: String, partition: Int, offset: Long)


class RichConfig(rootConfig: Config) {

  def getResumeConfig(path: String): util.Map[String, ResumeConfig] = {
    val configList = rootConfig.getConfigList(path)

    //MapBuilder
    val b = immutable.Map.newBuilder[String, ResumeConfig]
    configList.forEach { c ⇒
      val groupId = c.getString("groupId")
      val topic = c.getString("topic")
      val partition = c.getInt("partition")
      val offset = c.getLong("offset")
      val key = CommonUtil.combineResumeKey(groupId, topic, partition)

      b += (key → ResumeConfig(groupId, topic, partition, offset))
    }
    b.result().asJava
  }
}

//提供隐式转换 func 来将原有类型转换为Rich 类型
object RichConfig {
  implicit def convert(rootConfig: Config): RichConfig = new RichConfig(rootConfig)
}



