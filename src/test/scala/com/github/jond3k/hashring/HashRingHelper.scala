package com.github.jond3k.hashring

import collection.mutable

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
trait HashRingHelper {
  // use strings as a type as they should already generate good hash code entropy
  val defaultValueHashFun = new HashRing[String, String](Set.empty).valueHashFun
  val defaultKeyHashFun   = new HashRing[String, String](Set.empty).keyHashFun

  def ring(ids: Seq[Int]) = {
    new HashRing[Int, Int](ids.toSet, valueHashFun = (k, r) => k.hashCode)
  }

  def defaultHashRing(ids: Seq[Int], replicas: Int = 1) = {
    new HashRing[Int, Int](ids.toSet, replicas = replicas)
  }

  def lookupsFor(range: Seq[Int], ring: HashRing[Int, Int]) = {
    range.map(i => ring(i))
  }

  def frequenciesOf(seq: IndexedSeq[Int], ring: HashRing[Int, Int]): Map[Int, Int] = {
    val results = mutable.Map.empty[Int, Int].withDefaultValue(0)
    seq.foreach(n => {
      val key = ring(n)
      results(key) = results(key) + 1
    })
    results.toMap
  }

  def distributionOf(seq: IndexedSeq[Int], ring: HashRing[Int, Int]): Map[Int, Float] = {
    val frequencies = frequenciesOf(seq, ring)
    val total       = seq.length
    val results     = mutable.Map.empty[Int, Float].withDefaultValue(0)
    frequencies.foreach({
      case (k: Int, v: Int) => results(k) = v / total
    })
    results.toMap
  }
}
