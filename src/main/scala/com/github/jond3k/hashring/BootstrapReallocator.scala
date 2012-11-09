package com.github.jond3k.hashring

import annotation.tailrec

/**
 * Equally distribute all buckets in the ring. Original positions may be completely lost so this is best only used for
 * bootstrapping. This algorithm runs in O(n)
 *
 * @author jonathan davey <jond3k@gmail.com>
 */
class BootstrapReallocator(nodes: List[HashRingNode], ring: Array[HashRingNode]) extends Reallocator {
  assert(ring.length > 0)
  assert(nodes.length > 0)
  val bucketsPerNode: Int = math.round(ring.length / nodes.length).ensuring(_ > 0)

  @tailrec
  private[this] def allocate(nodes: List[HashRingNode], iterator: Int, remainingThisNode: Int) {
    ring(iterator) = nodes.head
    if      (iterator == ring.length) Unit
    else if (remainingThisNode == 0)  allocate(nodes.tail, iterator, bucketsPerNode)
    else                              allocate(nodes, iterator + 1, remainingThisNode - 1)
  }

  def allocate() {
    allocate(nodes, 0, bucketsPerNode)
  }

}
