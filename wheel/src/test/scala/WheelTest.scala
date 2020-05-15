import java.text.SimpleDateFormat
import java.util.Date

import com.github.ubiquitous.wheel.WheelFactory
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

    val task = new com.github.ubiquitous.wheel.Task[Unit](23) {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }
    }

    val task1 = new com.github.ubiquitous.wheel.Task[Unit](2) {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }
    }

    val task2 = new com.github.ubiquitous.wheel.Task[Unit](60) {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }
    }
    val task3 = new com.github.ubiquitous.wheel.Task[Unit](0) {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }
    }
    val task4 = new com.github.ubiquitous.wheel.Task[Unit](65) {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }
    }


    WheelFactory.addDelayTask(task)
    WheelFactory.addDelayTask(task1)
    WheelFactory.addDelayTask(task2)
    WheelFactory.addDelayTask(task3)
    WheelFactory.addDelayTask(task4)
    while (true) {

    }
  }

  test("wheelSizeTask") {

    val tasks = (0 until 100).map(i => new com.github.ubiquitous.wheel.Task[Unit](i) {
      override def call(): Unit = {
        println(s" after ${(new Date().getTime - startDate.getTime) / 1000} seconds , ** $i finished ** ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.Sss").format(new Date())}")
      }
    })

    tasks.foreach(WheelFactory.addDelayTask)


    Thread.sleep(120000)
  }

}
