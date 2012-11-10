package com.github.jond3k.hashring

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class BootstrapReallocatorTest extends FlatSpec with MustMatchers with ReallocatorHelper {

  it must "fail when given zero buckets" in {
    intercept[AssertionError](bootstrapping(nodes(1), ring(0)))
  }

  it must "fail when given zero nodes" in {
    intercept[AssertionError](bootstrapping(nodes(0), ring(1)))
  }

  it must "assign all buckets to a single node" in {
    bootstrapping(nodes(1), ring(10)) must equal (ring(1, 10))
  }

  it must "split buckets 5/5 given 2 nodes and 10 buckets" in {
    bootstrapping(nodes(2), ring(10)) must equal (ring(1, 5) ++ ring(2, 5))
  }

  it must "split buckets 4/3/3 given 3 nodes and 10 buckets" in {
    bootstrapping(nodes(3), ring(10)) must equal (ring(1, 4) ++ ring(2, 3) ++ ring(3, 3))
  }

}
