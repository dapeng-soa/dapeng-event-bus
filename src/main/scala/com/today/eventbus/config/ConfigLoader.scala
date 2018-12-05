package com.today.eventbus.config

import java.io.File
import java.util

import com.today.eventbus.common.SysEnvUtil
import com.typesafe.config.ConfigFactory
import com.today.eventbus.config.RichConfig._
import com.today.eventbus.utils.Constant
import org.slf4j.LoggerFactory

/**
  *
  * @author <a href=mailto:leihuazhe@gmail.com>maple</a>
  * @since 2018-12-05 10:57 AM
  */
object ConfigLoader {
  private val log = LoggerFactory.getLogger(getClass)


  val resumeConfigList = load(SysEnvUtil.MSG_BACK_TRACKING)

  /**
    * 根据指定路径，对 消息回溯 配置进行 load
    */
  def load(path: String): util.Map[String, ResumeConfig] = {
    try {
      val target = new File(path)
      val resumes = ConfigFactory.parseFile(target).getResumeConfig(Constant.MSG_BACK_TRACKING)
      deleteDir(target)
      resumes
    } catch {
      case e: Exception ⇒
        log.error(s"Load 消息回溯配置失败,可能指定路径 application.conf 文件不存在或配置存在问题,忽略. Cause: ${e.getMessage}")
        new util.HashMap[String, ResumeConfig]()
    }
  }

  private def deleteDir(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteDir)
    }
    file.delete()
    log.info("删除消息回溯的配置文件,只允许一次性使用,路径: " + file.getAbsolutePath)
  }


  def main(args: Array[String]): Unit = {
    println(resumeConfigList)

    println(resumeConfigList)
  }


}
