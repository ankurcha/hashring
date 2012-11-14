package com.github.jond3k.hashring

import java.util

/**
 * An consistent hash ring, inspired by: http://www.lexemetech.com/2007/11/consistent-hashing.html
 *
 * The benefits this has over the original are:
 *
 * - immutability
 * - fewer lookups
 * - separate key and value hash functions
 *
 * @author jonathan davey <jond3k@gmail.com>
 */
class HashRing[K, V](val values: List[V],
                     val keyHashFun:   (K)      => Int = { key: K => key.hashCode() },
                     val valueHashFun: (V, Int) => Int = { (value: V, replica: Int) => (value.toString + replica * 10).hashCode },
                     val replicas: Int = 1) {

  final protected val ring: util.SortedMap[Int, V] = new util.TreeMap[Int, V]()

  values.foreach(node => {
    (1 to replicas) foreach (replica => ring.put(valueHashFun(node, replica), node))
  })

  /**
   * Create a new HashRing with fewer nodes
   */
  def remove(removeNodes: List[V]): HashRing[K, V] = {
    if (removeNodes.intersect(values) != removeNodes) {
      throw new IllegalArgumentException("attempted to remove values that are not already present")
    }
    new HashRing(values.diff(removeNodes), keyHashFun, valueHashFun, replicas)
  }

  /**
   * Create a new HashRing with more nodes
   */
  def add(addNodes: List[V]): HashRing[K, V] = {
    if (!addNodes.intersect(values).isEmpty) {
      throw new IllegalArgumentException("attempted to add values that are already present")
    }
    new HashRing(values ++ addNodes, keyHashFun, valueHashFun, replicas)
  }

  /**
   * Make a clone of this HashRing but give it new nodes
   */
  def replace(values: List[V]): HashRing[K, V] = {
    new HashRing(values, keyHashFun, valueHashFun, replicas)
  }

  /**
   * Make an identical clone of this HashRing
   */
  override def clone(): HashRing[K, V] = {
    new HashRing(values, keyHashFun, valueHashFun, replicas)
  }

  /**
   * Get the next hash code after a position in the ring
   */
  final protected def nextValueHash(keyHash: Int) = ring.tailMap(keyHash) match {
    // wrap around if the tail map is empty
    case t if t.isEmpty => ring.firstKey()
    // use the next key if we're lucky
    case t => t.firstKey()
  }

  /**
   * Get the next value after a position in the ring
   */
  final protected def nextValue(keyHash: Int) = ring.get(nextValueHash(keyHash))

  /**
   * Get the value for this key
   */
  def apply(key: K): V = {
    if (ring.isEmpty) {
      throw new IllegalArgumentException("no values are currently present in the hash ring")
    }
    nextValue(keyHashFun(key))
  }

  /**
   * Get the value for this key, apply() is preferred.
   */
  def get(key: K): V = apply(key)

}
