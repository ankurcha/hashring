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
  // equally distribute buckets
  val bucketsPerNode: Int = math.floor(ring.length / nodes.length).toInt.ensuring(_ > 0)

  // we can't always bootstrapping things out equally. give the remainder to the first node
  val remainder     = ring.length % nodes.length
  val remainderNode = nodes.head

  @tailrec
  private[this] def allocate(nodes: List[HashRingNode], iterator: Int, remainingThisNode: Int) {
    if (iterator < ring.length) {
      ring(iterator) = nodes.head
      if (remainingThisNode == 0) allocate(nodes.tail, iterator, bucketsPerNode)
      else                        allocate(nodes, iterator + 1, remainingThisNode - 1)
    }
  }

  @tailrec
  private[this] def allocateRemainder(node: HashRingNode, remaining: Int, iterator: Int = 0) {
    if (remaining > 0) {
      ring(iterator) = node
      allocateRemainder(node, remaining - 1, iterator + 1)
    }
  }

  def allocate() {
    allocateRemainder(remainderNode, remainder)
    allocate(nodes, remainder, bucketsPerNode)
  }

}
