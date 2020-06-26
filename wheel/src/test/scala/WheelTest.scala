import java.text.SimpleDateFormat
import java.util.Date

import com.github.ubiquitous.wheel.{Task, WheelFactory}
import org.scalatest.FunSuite

/**
  *
  * @author Namhwik on 2020-04-15 17:12
  */
class WheelTest extends FunSuite {
  val startDate = new Date()

  test("start") {
    //WheelFactory.start()
    println(s"  ** start ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(startDate)}")

    val task = new com.github.ubiquitous.wheel.Task[Unit] {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }

      override def persist(): Boolean = {
        true
      }

      override def dl: Int = 23
    }

    val task1 = new com.github.ubiquitous.wheel.Task[Unit] {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }

      override def persist(): Boolean = {
        true
      }

      override def dl: Int = 2
    }

    val task2 = new com.github.ubiquitous.wheel.Task[Unit] {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }

      override def persist(): Boolean = {
        true
      }

      override def dl: Int = 60
    }
    val task3 = new com.github.ubiquitous.wheel.Task[Unit] {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }

      override def persist(): Boolean = {
        true
      }

      override def dl: Int = 0
    }
    val task4 = new com.github.ubiquitous.wheel.Task[Unit] {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }

      override def dl: Int = 65

      override def persist(): Boolean = {
        true
      }
    }

    Array(task, task1, task2, task3, task4).foreach(WheelFactory.addDelayTask)
    while (true) {

    }
  }

  test("wheelSizeTask") {
    case class TK(delay: Int, var msg: String) extends Task[Unit] {
      override def dl: Int = delay

      override def persist(): Boolean = false

      override def call(): Unit = {
        //Thread.sleep(delay * 1000)
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** $msg finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }

      override def equals(obj: Any): Boolean = super.equals(obj)
    }
    //val tasks = (0 until 1000).map(i => TK(i, i.toString))

    (0 until 1000).foreach(
      i => {
        Thread.sleep(i * 1000)
        WheelFactory.addDelayTask(TK(60, s"$i ...."))
      }
    )

    // tasks.foreach(WheelFactory.addDelayTask)


    Thread.sleep(7200000)
  }

}
