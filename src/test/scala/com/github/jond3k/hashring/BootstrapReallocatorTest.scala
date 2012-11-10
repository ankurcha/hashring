package com.github.jond3k.hashring

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class BootstrapReallocatorTest extends FlatSpec with MustMatchers {

  def nodes(count: Int) = (1 to count).map(i => HashRingNode(i)).toList

  def ring(size: Int) = new Array[HashRingNode](size)

  def ring(id: Int, size: Int) = (1 to size).map(i => HashRingNode(id)).toArray

  def balance(nodes: List[HashRingNode], ring: Array[HashRingNode]) = {
    (new BootstrapReallocator(nodes, ring)).allocate()
    ring
  }

  it must "fail when given zero buckets" in {
    intercept[AssertionError](balance(nodes(1), ring(0)))
  }

  it must "fail when given zero nodes" in {
    intercept[AssertionError](balance(nodes(0), ring(1)))
  }

  it must "assign all buckets to a single node" in {
    balance(nodes(1), ring(10)) must equal (ring(1, 10))
  }

  it must "split buckets 5/5 given 2 nodes and 10 buckets" in {
    balance(nodes(2), ring(10)) must equal (ring(1, 5) ++ ring(2, 5))
  }

  it must "split buckets 4/3/3 given 3 nodes and 10 buckets" in {
    balance(nodes(3), ring(10)) must equal (ring(1, 4) ++ ring(2, 3) ++ ring(3, 3))
  }

  it must "split buckets 34/33/33 given 3 nodes and 100 buckets" in {
    balance(nodes(3), ring(100)) must equal (ring(1, 34) ++ ring(2, 33) ++ ring(3, 33))
  }

}
