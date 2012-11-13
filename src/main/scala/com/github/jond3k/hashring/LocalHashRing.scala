package com.github.jond3k.hashring

import annotation.tailrec

/**
 *
 * @author jonathan davey <jond3k@gmail.com>
 */
class LocalHashRing[A](val nodes: IndexedSeq[HashRingNode],
                       val hashFun: (A) => Int = { key: A => key.hashCode() } ) {


  @tailrec
  final def nodeAt(i: Int): HashRingNode = {
    if (nodes.size == 0) {
      throw new IllegalArgumentException("no nodes currently registered")
    }

    Option(nodes(i % nodes.length)) match {
      case Some(o) => o
      case None    => nodeAt(i + 1)
    }
  }

  def nodeFor(key: A): HashRingNode = {
    val hash = hashFun(key)
    val node = nodeAt(hash)
    node
  }

  def add(addNodes: IndexedSeq[HashRingNode]): LocalHashRingTest[A] = {
    if (!addNodes.intersect(nodes).isEmpty) {
      throw new IllegalArgumentException("attempted to add nodes that are already registered")
    }
    new LocalHashRingTest[A](nodes ++ addNodes, hashFun)
  }

  def remove(removeNodes: IndexedSeq[HashRingNode]): LocalHashRingTest[A] = {
    if (removeNodes.intersect(nodes) != removeNodes) {
      throw new IllegalArgumentException("attempted to remove nodes that are not already registered")
    }
    new LocalHashRingTest[A](nodes.diff(removeNodes), hashFun)
  }

}
