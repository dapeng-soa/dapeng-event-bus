import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.atomic.AtomicLong

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource
import com.today.eventbus.scheduler.MsgPublishTask
import wangzx.scala_commons.sql._

object ProducerTest {
  private val atomicLong = new AtomicLong(0)

  private val dataSource = {
    val ds = new MysqlDataSource
    ds.setURL(s"jdbc:mysql://115.159.41.97:3306/maple?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull")
    ds.setUser("root")
    ds.setPassword("123456")
    ds
  }

  def main(args: Array[String]): Unit = {
    val msgPublishTask: MsgPublishTask = new MsgPublishTask("struy", "localhost:9092", "trans_", dataSource)
    msgPublishTask.startScheduled()

    //业务
    val in = new BufferedReader(new InputStreamReader(System.in))
    do {
      val input = in.readLine
      if (input == "quit") System.exit(0)
      if (!("" == input)) {
        dataSource.executeUpdate(sql"""insert into dp_common_event set event_type = $input ,event_binary=${input.getBytes} """)
      }
    } while (true)

  }
}
