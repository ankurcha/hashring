package com.github.jond3k.hashring

import annotation.tailrec

/**
 * Existing nodes are to be removed from the ring and their buckets are to be redistributed amongst the remaining ones.
 * This algorithm runs in O(2n)
 */
class RemoveReallocator(remaining: List[HashRingNode], remove: HashRingNode, ring: Array[HashRingNode]) extends Reallocator {
  assert(ring.length > 0)
  assert(remaining.length > 0)
  assert(!remaining.contains(remove))

  @tailrec
  private[this] def release(iterator: Int = 0, released: Int = 0): Int = {
    if (iterator == ring.length) released
    else if (remove == ring(iterator)) {
      ring(iterator) = null
      release(iterator + 1, released + 1)
    }
    else release(iterator + 1, released)
  }

  @tailrec
  private[this] def redistribute(to: List[HashRingNode], amountEachNode: Int, amountThisNode: Int = 0, iterator: Int = 0) {
    if (iterator == ring.length) Unit
    else if (ring(iterator) != null) {
      redistribute(to.tail, amountEachNode, amountThisNode, iterator + 1)
    } else if (amountThisNode < amountEachNode) {
      ring(iterator) = to.head
      redistribute(to.tail, amountEachNode, amountThisNode + 1, iterator + 1)
    } else {
      ring(iterator) = to.head
      redistribute(to.tail, amountEachNode, 0, iterator + 1)
    }
  }

  def allocate() {
    val released = release()
    val perNode  = released / remaining.length
    redistribute(remaining, perNode)
  }
}


