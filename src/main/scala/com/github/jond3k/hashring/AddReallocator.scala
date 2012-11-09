package com.github.jond3k.hashring
import scala.collection.mutable

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class AddReallocator(existing: List[HashRingNode], adding: HashRingNode, ring: Array[HashRingNode]) extends Reallocator {
  assert(existing.length > 0)
  assert(ring.length > 0)
  assert(!existing.contains(adding))

  val newNodeCount                = existing.length + 1
  val existingBucketsPerNode: Int = math.round(ring.length / existing.length).ensuring(_ > 0)
  val newBucketsPerNode: Int      = math.round(ring.length / newNodeCount).ensuring(_ > 0)

  val reallocated = mutable.Map[HashRingNode, Int](existing.map((_ -> 0)) : _*)

  def allocate() {

  }

}
