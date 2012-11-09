package com.github.jond3k.hashring

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class HashRing[A](
                   val hashFun: (A) => Int,
                   val nodes: List[HashRingNode],
                   val original: Option[HashRing[A]] = Option.empty,
                   val numBuckets: Int = 256) {

  if (numBuckets < 1)
    throw new IllegalArgumentException("numBuckets must be at least 1")

  if (original.isDefined && numBuckets != original.get.numBuckets)
    throw new IllegalArgumentException("Original HashRing had %s buckets but %s was expected" format (original.get.numBuckets, numBuckets))

  if (nodes.isEmpty)
    throw new IllegalArgumentException("Your cluster must consist of at least one node")

  protected def initialiseBuckets(): Array[HashRingNode] = original match {
    case Some(o) => o.buckets.clone()
    case None    => new Array[HashRingNode](numBuckets)
  }

  val (removedFromOriginal, addedToOriginal) = addedAndRemoved()

  val nodesInOriginal = if (original.isDefined) original.get.nodes else List.empty[HashRingNode]

  val bucketsPerNode: Int = math.round(numBuckets / nodes.length)

  val bucketsPerNodeDifferentFromOriginal = if (original.isDefined) original.get.bucketsPerNode - bucketsPerNode else 0

  val bucketsTakenByNodesAdded = bucketsPerNodeDifferentFromOriginal.max(0)


  val bucketsGivenByNodesRemoved = bucketsPerNodeDifferentFromOriginal.min(0)

  val buckets = {
    initialiseBuckets()
  }


  private def addedAndRemoved(): (List[HashRingNode], List[HashRingNode]) = {
    original match {
      case Some(o) => {
        val added   = o.nodes.diff(nodes)
        val removed = nodes.diff(o.nodes)
        (added, removed)
      }
      case None => (nodes, List.empty)
    }
  }

  def apply(bucket: Int): Option[HashRingNode] = {
    if (bucket < 0 || bucket >= numBuckets)
      throw new IllegalArgumentException("The value %s is outside of bucket range 0..%s" format (bucket, numBuckets-1))
    Option(buckets(bucket))
  }

  def nodeFor(key: A): HashRingNode = {
    val hash   = hashFun(key)
    val bucket = hash % numBuckets
    this(bucket).getOrElse(
      throw new Exception("Bucket %s has no node; ring not initialized correctly" format bucket))
  }

  def replace(nodes: List[HashRingNode]) = {
    new HashRing(hashFun, nodes, Option(this))
  }

}
