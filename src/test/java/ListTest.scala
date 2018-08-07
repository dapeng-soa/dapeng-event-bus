/**
  * desc: main
  *
  * @author hz.lei
  * @since 2018年08月03日 上午9:53
  */
object ListTest {

  def main(args: Array[String]): Unit = {
    val ids = List(1, 2, 3, 4, 5)
    println(ids.nonEmpty)
    println(ids.size > 0)

    val tests = List()

    println(tests.nonEmpty)
    println(tests.size > 0)
  }

}
