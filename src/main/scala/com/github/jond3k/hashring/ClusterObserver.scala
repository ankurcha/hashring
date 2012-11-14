package com.github.jond3k.hashring

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
trait ClusterObserver[K] {
  def onRingRebalance(hashRing: HashRing[K, Instance])
}
