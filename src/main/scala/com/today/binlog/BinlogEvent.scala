package com.today.binlog


import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowData}
import spray.json.{DefaultJsonProtocol, JsValue}
import spray.json._

import scala.collection.JavaConverters._

/**
  * binlog事件模型
  *
  * @param schema    库名
  * @param tblName   表名
  * @param eventType 数据变动类型
  * @param timestamp 事件发生的时间戳, 毫秒
  * @param before    变动前的值
  * @param after     变动后的值
  * @author hz.lei
  * @date 2018年03月08日 上午12:42
  */
case class BinlogEvent(schema: String, tblName: String, eventType: EventType,
                       timestamp: Long, before: JsValue, after: JsValue)


object BinlogEvent extends DefaultJsonProtocol {

  def apply(message: Array[Byte]): java.util.List[BinlogEvent] = {
    message
  }

  implicit def byteBuffer2BinlogEvent(message: Array[Byte]): java.util.List[BinlogEvent] = {
    //    val bytes: Array[Byte] = new Array[Byte](message.remaining)
    //    message.get(bytes, 0, bytes.length)

    val binlog: CanalEntry.Entry = CanalEntry.Entry.parseFrom(message)

    val rowChange: CanalEntry.RowChange = CanalEntry.RowChange.parseFrom(binlog.getStoreValue())

    val execTime = binlog.getHeader().getExecuteTime()
    val eventType: CanalEntry.EventType = binlog.getHeader().getEventType()
    val schema = binlog.getHeader().getSchemaName()
    val tblName = binlog.getHeader().getTableName()

    rowChange.getRowDatasList().asScala.map { rowData =>
      val before = getBeforeColumnsList(rowData)
      val after = getAfterColumnsList(rowData)

      BinlogEvent(schema, tblName, eventType,
        execTime, before, after)
    }.asJava
  }

  /**
    * new change
    *
    * @param rowData
    * @return
    * @date 2018.04.02
    */
  private def getBeforeColumnsList(rowData: RowData): JsValue =
    rowData.getBeforeColumnsList.asScala.map(column => (column.getName, convert(column.getValue, column.getSqlType))).toMap.toJson


  private def getAfterColumnsList(rowData: RowData): JsValue =
    rowData.getAfterColumnsList.asScala.map(column => (column.getName, convert(column.getValue, column.getSqlType))).toMap.toJson


  /**
    * convert
    */
  private def convert(value: String, sqlType: Int): Any = {
    sqlType match {
      case I_COLUMN_TYPE_INT | I_COLUMN_TYPE_TINYINT | I_COLUMN_TYPE_SMALLINT => value.toInt
      case I_COLUMN_TYPE_BIGINT => value.toLong
      case I_COLUMN_TYPE_VARCHAR | I_COLUMN_TYPE_TEXT => value
      case I_COLUMN_TYPE_DATETIME => value
      case I_COLUMN_TYPE_BIT => value
      case I_COLUMN_TYPE_FLOAT | I_COLUMN_TYPE_DOUBLE => value.toDouble
      case _ => value
    }
  }

  val I_COLUMN_TYPE_INT = 4
  val I_COLUMN_TYPE_BIGINT = -5
  val I_COLUMN_TYPE_VARCHAR = 12
  val I_COLUMN_TYPE_DATETIME = 93
  val I_COLUMN_TYPE_BIT = -7
  val I_COLUMN_TYPE_SMALLINT = 5
  val I_COLUMN_TYPE_FLOAT = 7
  val I_COLUMN_TYPE_DOUBLE = 8
  val I_COLUMN_TYPE_TIMESTAMP = 93
  val I_COLUMN_TYPE_TEXT = 2005
  val I_COLUMN_TYPE_LONGTEXT = 2005
  val I_COLUMN_TYPE_BLOB = -4
  val I_COLUMN_TYPE_YEAR = 12
  val I_COLUMN_TYPE_TIME = 92
  val I_COLUMN_TYPE_TINYINT = -6

}