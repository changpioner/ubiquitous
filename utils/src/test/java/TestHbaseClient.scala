import com.github.ubiquitous.utils.HbaseUtil
import org.scalatest.FunSuite

/**
  *
  * @author Namhwik on 2020-05-13 22:07
  */
class TestHbaseClient extends FunSuite {

  test("insert") {
    HbaseUtil.insert("schedule_cache", "row001", "f1", "c1", "v0001")
  }

}
