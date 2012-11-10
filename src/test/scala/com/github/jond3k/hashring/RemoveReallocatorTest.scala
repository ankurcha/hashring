package com.github.jond3k.hashring

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class RemoveReallocatorTest extends FlatSpec with MustMatchers with ReallocatorHelper {

  it must "fail when given zero buckets" in {
    intercept[AssertionError](removing(node(), ring(0)))
  }

  it must "fail when there are zero surviving nodes" in {
    intercept[AssertionError](removing(node(), ring(10)))
  }

  it must "reallocate 5/5 to 10" in {
    removing(node(2), bootstrapping(nodes(2), ring(10))) must equal (ring(1, 10))
  }

  it must "reallocate 4/3/3 to 5/5" in {
    def bootstrapped = bootstrapping(nodes(3), ring(100))
    counts(removing(node(1), bootstrapped)) must equal (Map(node(2) -> 50, node(3) -> 50))
    counts(removing(node(2), bootstrapped)) must equal (Map(node(1) -> 51, node(3) -> 49)) // FIXME
    counts(removing(node(3), bootstrapped)) must equal (Map(node(1) -> 51, node(2) -> 49)) // FIXME
  }

  it must "reallocate 4/2/2/2 to 4/3/3" in {
    def bootstrapped = bootstrapping(nodes(4), ring(100))
    counts(removing(node(1), bootstrapped)) must equal (Map(node(2) -> 34, node(3) -> 33, node(4) -> 33))
    counts(removing(node(2), bootstrapped)) must equal (Map(node(1) -> 34, node(3) -> 33, node(4) -> 33))
    counts(removing(node(3), bootstrapped)) must equal (Map(node(1) -> 34, node(2) -> 33, node(4) -> 33))
  }

}
