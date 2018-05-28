package com.today.binlog

import java.util

/**
  *
  * 描述:
  *
  * @author hz.lei
  * @since 2018年03月08日 上午1:00
  */
object BinlogMsgProcessor {

  def process(message: Array[Byte]): util.List[BinlogEvent] = {
    BinlogEvent.apply(message)
  }

}
