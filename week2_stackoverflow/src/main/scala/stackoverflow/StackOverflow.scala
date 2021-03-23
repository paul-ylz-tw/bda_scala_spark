package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import annotation.tailrec

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    vectors.cache() // significant speed up from this, 16 min vs 4 min
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000

  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** PART 1: Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(_.postingType == 1).map(p => (p.id, p))
    // get is required to convert Option to Int
    val answers = postings.filter(_.postingType == 2).map(p => (p.parentId.get, p))
    // (inner) join gives us only the questions with answers and those answers
    // groupByKey gives us the Iterable 2nd tuple item
    questions.join(answers).groupByKey()
  }


  /** PART 2: Compute the maximum score for each posting - that is, for each question, among all the answers given,
    * what was the highest score held by any one answer? (scores are based on user votes)
    * */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    /** We want the form of (1, 6,   None, None, 140, Some(CSS)),  67) per element of our RDD.
      * The first tuple item is the question, the second is the high score.
      * Question is obtained from the first element of the iterable's first tuple.
      * High score is obtained from scanning all the answers within the iterable.
      * Casting to an array is required to use the provided answerHighScore method.
      * */
    grouped.map(v => (
      v._2.head._1, // the question
      answerHighScore(v._2.map(_._2).toArray) // the highest scored answer to the question
    ))
  }


  /** PART 3: Compute the vectors for the kmeans
    * We want to convert scores, of the form (Question, HighScore)
    * into vectors, of the form (LangIndex, HighScore)
    * So basically we have to map Questions to a language index while preserving the HighScore part of the tuple.
    * */
  //    RDD(lang index * lang spread, high score)
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`.
      * Why does the question want to use a recursive function to do this?!
      * */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored.map(question_HiScore => (
      firstLangInTag(question_HiScore._1.tags, langs).get * langSpread,
      question_HiScore._2
    ))
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
      // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
      // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** PART 4: Main kmeans computation
    * The solution should use the methods:
    * findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int
    * averageVectors(ps: Iterable[(Int, Int)]): (Int, Int)
    * euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double
    *
    * Directions are to:
    * 1. pair each vector with the index of the closest mean (its cluster)
    * * this means we want an RDD of pairs containing RDD[(vector, closest mean)]
    *
    * 2. compute the new means by averaging the values of each cluster.
    * This is of the form Array[(Int, Int)]
    *
    * */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    // gives us RDD[(Int, (Int, Int))] which is RDD[(idx of mean, vector)]
    val pairedWithClosestMean = vectors.map(v => (findClosest(v, means), v))

    // since averageVectors takes an Iterable, suggests we need to use groupByKey
    // gives us RDD[(Int, Iterable(Int, Int))] which is RDD[(idx of mean, vectors closest to that mean)]
    val groupByMeanIndex = pairedWithClosestMean.groupByKey()

    //    Calculate the new averages of vectors for each mean. Each new average will become the new mean for the next iteration.
    //    mapValues allows us to do the averaging operation on the vectors while maintaining reference to the mean's index.
    //    Last, we convert to a map to make it easy to look up the new mean value for the old mean (if it's present)
    //    When would it not be present?
    //    If the mean was never closest to any of the points. It would be left out during the pairing with closest means.
    val newMeansMap = groupByMeanIndex.mapValues(vectors => averageVectors(vectors)).collect().toMap

    //    if the new mean does not exist in the map, then the old mean stays put (ie, use the original mean value)
    val newMeans = means.indices.map(i => newMeansMap.getOrElse(i, means(i))).toArray

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(
        s"""Iteration: $iter
           |  * current distance: $distance
           |  * desired distance: $kmeansEta
           |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }


  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length,
      "Failure in euclideanDistance, expected arguments to have same length but got lengths " + a1.length +
        " and " + a2.length)
    var sum = 0d
    var idx = 0
    while (idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Returns the index of the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  /** PART 5 Displaying results:
    * (a) the dominant programming language in the cluster;
    * (b) the percent of answers that belong to the dominant language;
    * (c) the size of the cluster (the number of questions it contains);
    * (d) the median of the highest answer scores.
    * */
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    //    pair every vector to the index of it's closest mean
    val closest = vectors.map(p => (findClosest(p, means), p))
    //    group the vectors by the index of their closest mean
    //    ie: RDD[(indexOfMean, Iterable(vectors))]
    val closestGrouped = closest.groupByKey()

    //    now we calculate statistics for each cluster
    //    vs is of type Iterable[(LangIndex, HighScore)]
    val median = closestGrouped.mapValues { vs =>

      //      Map[(LangIndex, Iterable[(LangIndex, HighScore)])]
      val langScores = vs.groupBy(vector => vector._1)

      //      (LangIndex, Iterable[LangIndex, HighScore])
      val dominantLangV = langScores.maxBy(vector => vector._2.size)

      //      (a)  most common language in the cluster
      //      Fetch list item using parenthesis! wth.
      val langLabel: String = langs(dominantLangV._1 / langSpread)

      val dominantLangAnswerCount = dominantLangV._2.size
      val allLangAnswerCount = vs.size
      //      (b) percent of the questions in the most common language
      val langPercent: Double = dominantLangAnswerCount * 100 / allLangAnswerCount

      //      (c) the size of the cluster (the number of questions it contains);
      val clusterSize: Int = allLangAnswerCount

      //      (d) the median of the highest answer scores.
      val sortedHighScores = vs.map(v => v._2).toArray.sorted

      //      If a list of sorted numbers is even, then the median is the average of the middle 2 numbers.
      val medianScore: Int = if (sortedHighScores.length % 2 != 0) {
        sortedHighScores(sortedHighScores.size / 2)
      } else {
        (sortedHighScores(sortedHighScores.size / 2) + sortedHighScores(sortedHighScores.size / 2 - 1)) / 2
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    //    [("Scala", 0.6, 424, 15),...]
    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
