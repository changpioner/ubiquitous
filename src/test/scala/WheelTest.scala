import java.util.concurrent.{Executors, TimeUnit}

import com.github.ubiquitous.wheel.{RingBufferWheel, WheelFactory}
import org.scalatest.FunSuite

/**
  *
  * @author Namhwik on 2020-04-15 17:12
  */
class WheelTest extends FunSuite {
  test("start") {

    val task = new com.github.ubiquitous.wheel.Task(10) {
      override def run(): Unit = {
        println("running")
      }
    }

    WheelFactory.addDelayTask(task)
    while (true) {

    }
  }

}
