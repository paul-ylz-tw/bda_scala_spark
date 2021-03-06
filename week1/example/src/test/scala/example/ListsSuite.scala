package example

import org.junit._
import org.junit.Assert
import org.junit.Assert.{assertEquals, fail}

/**
  * This class implements a JUnit test suite for the methods in object
  * `Lists` that need to be implemented as part of this assignment. A test
  * suite is simply a collection of individual tests for some specific
  * component of a program.
  *
  * To run this test suite, start "sbt" then run the "test" command.
  */
class ListsSuite {

  /**
    * Tests are written using the @Test annotation
    *
    * The most common way to implement a test body is using the method `assert`
    * which tests that its argument evaluates to `true`. So one of the simplest
    * successful tests is the following:
    */
  @Test def `one plus one is two (0pts)`: Unit = {
    assert(1 + 1 == 2)
  }

  @Test def `one plus one is three (0pts)?`: Unit = {
    assert(1 + 2 == 3)
  }

  /**
    * One problem with the previous (failing) test is that JUnit will
    * only tell you that a test failed, but it will not tell you what was
    * the reason for the failure. The output looks like this:
    *
    * {{{
    *    [info] - one plus one is three? *** FAILED ***
    * }}}
    *
    * This situation can be improved by using a Assert.assertEquals
    * (this is only possible in JUnit). So if you
    * run the next test, JUnit will show the following output:
    *
    * {{{
    *    [info] - details why one plus one is not three *** FAILED ***
    *    [info]   2 did not equal 3 (ListsSuite.scala:67)
    * }}}
    *
    * We recommend to always use the Assert.assertEquals equality operator
    * when writing tests.
    */
  @Test def `details why one plus one is not three (0pts)`: Unit = {
    assertEquals(3, 1 + 1 + 1)
  }

  /**
    * Exceptional behavior of a methods can be tested using a try/catch
    * and a failed assertion.
    *
    * In the following example, we test the fact that the method `intNotZero`
    * throws an `IllegalArgumentException` if its argument is `0`.
    */
  @Test def `intNotZero throws an exception if its argument is 0`: Unit = {
    try {
      intNotZero(0)
      Assert.fail("No exception has been thrown")
    } catch {
      case e: IllegalArgumentException => ()
    }
  }

  def intNotZero(x: Int): Int = {
    if (x == 0) throw new IllegalArgumentException("zero is not allowed")
    else x
  }

  /**
    * Now we finally write some tests for the list functions that have to be
    * implemented for this assignment. We fist import all members of the
    * `List` object.
    */

  import Lists._


  /**
    * We only provide two very basic tests for you. Write more tests to make
    * sure your `sum` and `max` methods work as expected.
    *
    * In particular, write tests for corner cases: negative numbers, zeros,
    * empty lists, lists with repeated elements, etc.
    *
    * It is allowed to have multiple `assert` statements inside one test,
    * however it is recommended to write an individual `test` statement for
    * every tested aspect of a method.
    */
  @Test def `sum of a few numbers (10pts)`: Unit = {
    assertEquals(3, sum(List(1, 2, 0)))
  }

  @Test def `sum of both positive and negative numbers`: Unit = {
    assertEquals(2, sum(List(1, 2, 0, -1)))
  }

  @Test def `sum of repeated elements`: Unit = {
    assertEquals(12, sum(List(3, 3, 3, 3)))
  }

  @Test def `sum returns 0 when given an empty list`: Unit = {
    assertEquals(0, sum(List()))
  }

  @Test def `max of a few numbers (10pts)`: Unit = {
    assertEquals(7, max(List(3, 7, 2)))
  }

  @Test def `max of positive and negative integers`: Unit = {
    assertEquals(7, max(List(3, 7, 2, -7)))
  }

  @Test def `max of few numbers including zeros`: Unit = {
    assertEquals(7, max(List(3, 7, 0, 0)))
  }

  @Test def `throws NoSuchElementException with empty list`: Unit = {
    try {
      max(List())
      fail("Did not throw")
    } catch {
      case e: NoSuchElementException => ()
    }
  }


  @Rule def individualTestTimeout = new org.junit.rules.Timeout(1000)
}
