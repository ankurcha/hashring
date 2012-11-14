package com.github.jond3k.hashring

import org.apache.zookeeper.{CreateMode, ZooKeeper}
import annotation.tailrec

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class InstanceRegistrator(
                            zk: ZooKeeper,
                            instanceRoot: String,
                            instance: Instance) {

  def instancePath = instanceRoot + instance.id

  @tailrec final def waitUntilReady() {
    try {
      throwUnlessReady()
    } catch {
      case e: Exception => waitUntilReady()
    }
  }

  def throwUnlessReady() {
    zk.create(instancePath, instance.id.getBytes, null, CreateMode.EPHEMERAL)
  }
}
