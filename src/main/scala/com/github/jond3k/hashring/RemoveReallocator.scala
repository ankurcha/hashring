package com.github.jond3k.hashring

import annotation.tailrec

/**
 * Existing nodes are to be removed from the ring and their buckets are to be redistributed amongst the remaining ones.
 * This algorithm runs in O(2n): count released buckets and survived nodes, redistribute released buckets to survivors
 */
class RemoveReallocator(remove: HashRingNode, ring: Array[HashRingNode]) extends Reallocator {
  assert(ring.length > 0)

  @tailrec
  private[this] def countReleasedBucketsAndSurvivorNodes(
                                                          iterator: Int = 0,
                                                          released: Int = 0,
                                                          survivors: Set[HashRingNode] = Set.empty): (Int, Set[HashRingNode]) = {
    if (iterator < ring.length) {
      if (ring(iterator) == remove) countReleasedBucketsAndSurvivorNodes(iterator + 1, released + 1, survivors)
      else countReleasedBucketsAndSurvivorNodes(iterator + 1, released, survivors + ring(iterator))
    } else {
      released.ensuring(_ > 0) -> survivors.ensuring(_.size > 0)
    }
  }

  @tailrec
  private[this] def redistributeRemainder(
                                           node: HashRingNode,
                                           remaining: Int,
                                           iterator: Int = 0): Int = {
    if (remaining > 0) {
      if (ring(iterator) == remove) {
        ring(iterator) = node
        redistributeRemainder(node, iterator + 1, remaining - 1)
      } else {
        redistributeRemainder(node, iterator + 1, remaining)
      }
    } else {
      iterator
    }
  }

  @tailrec
  private[this] def redistributeToSurvivors(
                                             survivors: Set[HashRingNode],
                                             redistribute: Int,
                                             remaining: Int,
                                             iterator: Int = 0) {
    if (!survivors.isEmpty) {
      if (ring(iterator) == remove) {
        ring(iterator) = survivors.head
        if (remaining == 1) {
          redistributeToSurvivors(survivors.tail, redistribute, redistribute, iterator + 1)
        } else {
          redistributeToSurvivors(survivors, redistribute, remaining - 1, iterator + 1)
        }
      } else {
        redistributeToSurvivors(survivors, redistribute, remaining, iterator + 1)
      }
    }
  }


  def allocate() {

    val (released, survivors) = countReleasedBucketsAndSurvivorNodes()

    val redistribute  = math.floor(released / survivors.size).toInt.ensuring(_ > 0)
    val remainder     = released % survivors.size
    val remainderNode = survivors.head

    val lastRemainderIndex = redistributeRemainder(remainderNode, remainder)
    redistributeToSurvivors(survivors, redistribute, redistribute, lastRemainderIndex)
  }
}


