package com.today.eventbus.scheduler

import java.sql._

import javax.sql.DataSource
import org.slf4j.{Logger, LoggerFactory}
import wangzx.scala_commons.sql.{JdbcValue, ResultSetMapper, SQLWithArgs}

import scala.collection.mutable.ListBuffer

/**
  *
  * 描述:
  *
  * @author hz.lei
  * @date 2018年04月08日 下午4:52
  */
object ScalaSql {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def withTransaction[T](dataSource: DataSource)(f: Connection => T): T = {
    val conn = dataSource.getConnection
    try {
      conn.setAutoCommit(false)
      val result = f(conn)
      conn.commit
      result
    } catch {
      case ex: Throwable =>
        conn.rollback
        throw ex
    } finally {
      conn.close()
    }
  }

  def eachRow[T: ResultSetMapper](connection: Connection, sql: SQLWithArgs)(f: T => Unit) = {
    withPreparedStatement(sql.sql, connection) {
      prepared =>
        if (sql.args != null) setStatementArgs(prepared, connection, sql.args)

        logger.debug("SQL Preparing: {} args: {}", Seq(sql.sql, sql.args): _*)

        val mapper = implicitly[ResultSetMapper[T]]
        val rs = prepared.executeQuery()
        val rsMeta = rs.getMetaData
        while (rs.next()) {
          val mapped = mapper.from(rs)
          f(mapped)
        }
        logger.debug("SQL result: {}", rs.getRow)
    }

  }

  def executeUpdate(connection: Connection, stmt: SQLWithArgs): Int = executeUpdateWithGenerateKey(connection, stmt)(null)


  def rows[T: ResultSetMapper](conn: Connection, sql: SQLWithArgs): List[T] = withPreparedStatement(sql.sql, conn) { prepared =>
    val buffer = new ListBuffer[T]()
    if (sql.args != null) setStatementArgs(prepared, conn, sql.args)

    logger.debug("SQL Preparing: {} args: {}", Seq(sql.sql, sql.args): _*)

    val rs = prepared.executeQuery()
    val rsMeta = rs.getMetaData
    while (rs.next()) {
      val mapped = implicitly[ResultSetMapper[T]].from(rs)
      buffer += mapped

    }
    logger.debug("SQL result: {}", buffer.size)
    buffer.toList
  }


  private def withPreparedStatement[T](sql: String, conn: Connection)(f: PreparedStatement => T): T = {
    val stmt = conn.prepareStatement(sql)
    try {
      f(stmt)
    } finally {
      stmt.close()
    }
  }

  @inline private def setStatementArgs(stmt: PreparedStatement, conn: Connection, args: Seq[JdbcValue[_]]) =
    args.zipWithIndex.foreach {
      case (null, idx) => stmt.setNull(idx + 1, Types.VARCHAR)
      case (v, idx) => v.passIn(stmt, idx + 1)
    }


  def executeUpdateWithGenerateKey(conn: Connection, stmt: SQLWithArgs)(processGenerateKeys: ResultSet => Unit = null): Int = {
    val prepared = conn.prepareStatement(stmt.sql,
      if (processGenerateKeys != null) Statement.RETURN_GENERATED_KEYS
      else Statement.NO_GENERATED_KEYS)

    try {
      if (stmt.args != null) setStatementArgs(prepared, conn, stmt.args)

      logger.debug("SQL Preparing: {} args: {}", Seq(stmt.sql, stmt.args): _*)

      val result = prepared.executeUpdate()

      if (processGenerateKeys != null) {
        val keys = prepared.getGeneratedKeys
        processGenerateKeys(keys)
      }

      logger.debug("SQL result: {}", result)
      result
    }
    finally {
      prepared.close
    }
  }


}


