package com.github.jond3k.hashring

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers
import java.lang.IllegalArgumentException

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class LocalHashRingTest extends FlatSpec with MustMatchers with HashRingHelper {

  it must "fail when there are no nodes" in {
    intercept[IllegalArgumentException](hashing(10, Vector.empty))
  }

  it must "stripe nodes across a limited hash range" in {
    hashing(10, nodes(1))  must equal(stripe(10, nodes(1)))
    hashing(100, nodes(5)) must equal(stripe(100, nodes(5)))
  }

  it must "cluster keys for 1 missing node to the next remianing node in a limited hash range" in {
    hashing(10, nodes(1, 0))          must equal(stripe(10, nodes(1, 1)))
    hashing(10, nodes(0, 2))          must equal(stripe(10, nodes(2, 2)))
    hashing(18, nodes(0, 0, 3))       must equal(stripe(18, nodes(3, 3, 3)))
    hashing(20, nodes(0, 0, 3, 4))    must equal(stripe(20, nodes(3, 3, 3, 4)))
    hashing(20, nodes(1, 0, 0, 3, 4)) must equal(stripe(20, nodes(1, 3, 3, 3, 4)))
  }

  it must "fail when attempting to add existing nodes" in {
    intercept[IllegalArgumentException](adding(nodes(1), nodes(1)))
  }

  it must "fail when attempting to remove non-existant nodes" in {
    intercept[IllegalArgumentException](removing(nodes(1), nodes(2)))
  }

  it must "add new nodes to the end of the "

}
