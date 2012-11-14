package com.github.jond3k.hashring

import org.apache.zookeeper.{KeeperException, ZooKeeper}
import scala.collection.JavaConverters._
import annotation.tailrec

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class InstanceRebalancer[K](zk: ZooKeeper, instanceRoot: String, hashRing: HashRing[K, Instance]) {

  @tailrec def waitUntilRebalanced(): HashRing[K, Instance] = {
    try {
      throwUnlessRebalanced()
    } catch {
      case e: KeeperException => waitUntilRebalanced()
    }
  }

  def throwUnlessRebalanced(): HashRing[K, Instance] = {
    val children  = zk.getChildren(instanceRoot, true).asScala
    val names     = children.map(n => n.substring(n.lastIndexOf("/")))
    val instances = names.map(new Instance(_)).toList
    hashRing.replace(instances)
  }
}
