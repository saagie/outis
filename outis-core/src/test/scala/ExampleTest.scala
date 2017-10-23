import org.scalatest.{FlatSpec, Matchers}

class ExampleTest extends FlatSpec with Matchers {

  "1 + 1" should " equals 2" in {
    1 + 1 shouldBe 2
  }

  "2 + 1" should " equals 3" in {
    2 + 1 shouldBe 3
  }
}
