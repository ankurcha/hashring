package com.github.jond3k.hashring

import collection.mutable

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
trait ReallocatorHelper {

  def node(id: Int = 1) = HashRingNode(id)

  def nodes(count: Int) = (1 to count).map(node(_)).toList

  def ring(size: Int) = new Array[HashRingNode](size)

  def ring(id: Int, size: Int) = (1 to size).map(i => HashRingNode(id)).toArray

  def bootstrapping(nodes: List[HashRingNode], ring: Array[HashRingNode]) = {
    (new BootstrapReallocator(nodes, ring)).allocate()
    ring
  }

  def removing(node: HashRingNode, from: Array[HashRingNode]) = {
    (new RemoveReallocator(node, from)).allocate()
    from
  }

  def adding(to: Array[HashRingNode]) = {
    to
  }

  def counts(ring: Array[HashRingNode]) = {
    val results = mutable.Map.empty[HashRingNode, Int].withDefaultValue(0)
    ring.foreach(n => results(n) = results(n) + 1)
    results.toMap
  }

}
