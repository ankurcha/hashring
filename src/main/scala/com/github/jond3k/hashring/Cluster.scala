package com.github.jond3k.hashring

import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class Cluster[K](zk: ZooKeeper,
                 instance: Instance,
                 observers: Traversable[ClusterObserver[K]],
                 @volatile protected var _ring: HashRing[K, Instance]) extends Watcher {

  protected def ring_=(ring: HashRing[K, Instance]) {
    _ring = ring
    observers.foreach(_.onRingRebalance(ring))
  }

  protected def ring = _ring

  def throwUnlessRebalanced() {
    ring = (new InstanceRebalancer[K](zk, instanceRoot, ring)).throwUnlessRebalanced()
  }

  /**
   * Throws an exception if there were problems
   */
  def throwUnlessReady() {
    new InstanceRegistrator(zk, instanceRoot, instance).throwUnlessReady()
    throwUnlessRebalanced()

  }

  def waitUntilReady() {
    new InstanceRegistrator(zk, instanceRoot, instance).waitUntilReady()
    waitUntilRebalanced()
  }

  def waitUntilRebalanced() {
    ring = (new InstanceRebalancer[K](zk, instanceRoot, ring)).waitUntilRebalanced()
  }


  def instanceRoot = "/instances/"

  def isErrorEvent(event: WatchedEvent): Boolean = event.getState != 0

  /**
   * Returns true if this event is due to rebalancing
   */
  def isRebalanceEvent(event: WatchedEvent): Boolean = event.getPath.endsWith(instanceRoot)

  def this(zk: ZooKeeper, instance: Instance, observers: Traversable[ClusterObserver[K]]) {
    this (zk, instance, observers, new HashRing[K, Instance](List.empty))
  }

  def this(zkConnectString: String, zkSessionTimeout: Int, instance: Instance, observers: Traversable[ClusterObserver[K]]) {
    this(new ZooKeeper(zkConnectString, zkSessionTimeout, this), instance, observers)
  }

  def this(zkConnectString: String, zkSessionTimeout: Int, instance: Instance, observers: Traversable[ClusterObserver[K]], ring: HashRing[K, Instance]) {
    this(new ZooKeeper(zkConnectString, zkSessionTimeout, this), instance, observers, ring)
  }

  /**
   * Handles a ZooKeeper event or returns false if it's not something we expect
   *
   * This can be used to allow other ZooKeeper watchers to delegate events
   */
  def maybeProcess(event: WatchedEvent): Boolean = {
    if (isErrorEvent(event)) {
      waitUntilReady()
      true
    } else if(isRebalanceEvent(event)) {
      waitUntilRebalanced()
      true
    } else false
  }

  /**
   * Handle a ZooKeeper event or throw an exception if it's an unexpected event
   */
  def process(event: WatchedEvent) {
    if (!maybeProcess(event)) {
      throw new Exception("Unexpected event " + event)
    }
  }

}
