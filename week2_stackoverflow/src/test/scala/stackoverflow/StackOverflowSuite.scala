package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {
  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  //  case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable

  @Test def `groupedPostings groups questions and answers by question ID`: Unit = {
    val data = Seq(
      Posting(1, 123, Some(456), None, 0, Some("C#")),
      Posting(2, 234, None, Some(123), 5, Some("C#")),
      Posting(2, 456, None, Some(123), 11, Some("C#")),
      Posting(1, 1000, None, None, 0, Some("Java")),
      Posting(2, 1001, None, Some(1000), 5, Some("Java")),
      Posting(2, 1002, None, Some(1000), 11, Some("Java")),
      Posting(2, 1003, None, Some(1000), 12, Some("Java")),
    )
    val postingsRdd = sc.parallelize(data)
    val so = new StackOverflow
    val grouped = so.groupedPostings(postingsRdd).collect()
    assertEquals(2, grouped.length)
    assertEquals(123, grouped(0)._1)
    assertEquals(1000, grouped(1)._1)
    assertEquals(2, grouped(0)._2.toList.size)
    assertEquals(3, grouped(1)._2.toList.size)
  }


  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
