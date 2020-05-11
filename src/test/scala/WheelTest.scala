import com.github.ubiquitous.wheel.WheelFactory
import org.scalatest.FunSuite

/**
  *
  * @author Namhwik on 2020-04-15 17:12
  */
class WheelTest extends FunSuite {
  test("start") {

    val task = new com.github.ubiquitous.wheel.Task[Unit](65) {
      override def call(): Unit = {
        println("running")
      }
    }

    val task1 = new com.github.ubiquitous.wheel.Task[Unit](25) {
      override def call(): Unit = {
        println("running")
      }
    }

    val task2 = new com.github.ubiquitous.wheel.Task[Unit](1) {
      override def call(): Unit = {
        println("running 11")
      }
    }
    val task3 = new com.github.ubiquitous.wheel.Task[Unit](60) {
      override def call(): Unit = {
        println("running 11")
      }
    }


    WheelFactory.addDelayTask(task)
    WheelFactory.addDelayTask(task1)
    WheelFactory.addDelayTask(task2)
    WheelFactory.addDelayTask(task3)
    while (true) {

    }
  }

}
