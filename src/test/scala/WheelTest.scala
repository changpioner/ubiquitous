import java.util.concurrent.Executors

import com.github.ubiquitous.wheel.RingBufferWheel
import org.scalatest.FunSuite

/**
  *
  * @author Namhwik on 2020-04-15 17:12
  */
class WheelTest extends FunSuite {
  test("start") {
    val executorService = Executors.newFixedThreadPool(10)
    val ringBufferWheel = new RingBufferWheel(executorService, 4)
    ringBufferWheel.start()
    val task = new com.github.ubiquitous.wheel.Task(10) {
      override def run(): Unit = {
        println("running")
      }
    }

    ringBufferWheel.addTask(task)
    while (true) {

    }
  }

}
