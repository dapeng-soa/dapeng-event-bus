package com.today

/**
  *
  * Desc: TODO
  *
  * @author hz.lei
  * @date 2018年05月18日 下午3:16
  */
object Test extends App {

  val a: Option[Int] = Some(2)
  val b:Option[Int] = None

  println(a.isDefined)
  println(b.isDefined)

}
