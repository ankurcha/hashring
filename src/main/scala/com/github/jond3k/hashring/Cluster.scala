package com.github.jond3k.hashring

import org.apache.zookeeper._
import com.netflix.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import com.netflix.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import java.io.Closeable
import scala.collection.JavaConverters._
import com.netflix.curator.retry.ExponentialBackoffRetry
import annotation.tailrec
import org.slf4j.LoggerFactory

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class Cluster[K](curator: CuratorFramework,
                 instance: Instance,
                 observers: Traversable[ClusterObserver[K]],
                 @volatile protected var _ring: HashRing[K, Instance]) extends PathChildrenCacheListener with Closeable {

  private val log = LoggerFactory.getLogger(getClass)
  private val errorSleepTimeMs = 2000

  def this(instance: Instance, observers: Traversable[ClusterObserver[K]]) {
    this(CuratorFrameworkFactory.builder().connectString("localhost:2181").namespace("hashring").retryPolicy(new ExponentialBackoffRetry(1000, Int.MaxValue)).build(), instance, observers, new HashRing[K, Instance](Set.empty))
  }

  def this(curator: CuratorFramework, instance: Instance, observers: Traversable[ClusterObserver[K]]) {
    this (curator, instance, observers, new HashRing[K, Instance](Set.empty))
  }

  def this(zkConnectString: String, zkSessionTimeout: Int, zkNamespace: String, instance: Instance, observers: Traversable[ClusterObserver[K]]) {
    this(CuratorFrameworkFactory.builder().namespace(zkNamespace).connectString(zkConnectString).retryPolicy(new ExponentialBackoffRetry(1000, Int.MaxValue)).sessionTimeoutMs(zkSessionTimeout).build(), instance, observers)
  }

  def this(zkConnectString: String, zkSessionTimeout: Int, zkNamespace: String, instance: Instance, observers: Traversable[ClusterObserver[K]], ring: HashRing[K, Instance]) {
    this(CuratorFrameworkFactory.builder().namespace(zkNamespace).connectString(zkConnectString).retryPolicy(new ExponentialBackoffRetry(1000, Int.MaxValue)).sessionTimeoutMs(zkSessionTimeout).build(), instance, observers, ring)
  }

  def instanceRoot = "/instances"

  def instancePath = "%s/%s" format (instanceRoot, instance.id)

  val manuallyStarted = !curator.isStarted
  if (manuallyStarted) {
    curator.start()
    log.info("Started {}", curator)
  } else {
    log.info("Already started {}", curator)
  }

  createEntryRetryLoop()
  val cache = createCache()
  refresh()

  final protected def createEntryRetryLoop() {
    var complete = false
    while(!complete) {
      try {
        createEntry()
        complete = true
      } catch {
        case ex: KeeperException if ex.code() == KeeperException.Code.NODEEXISTS => {
          log.error("Instance ID might be duplicated", ex)
          Thread.sleep(errorSleepTimeMs)
        }
      }
    }
  }

  protected def createEntry() {
    curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(instancePath)
  }

  protected def refresh() {
    if (curator.isStarted) {
      ring = ring.replace(instances)
    }
  }

  protected def instancePaths = curator.getChildren.forPath(instanceRoot).asScala

  protected def ring_=(r: HashRing[K, Instance]) {
    _ring = r
    log.info("Cluster now consists of {}", ring.values)
    observers.foreach(_.onRingRebalance(ring))
  }

  protected def ring = _ring

  protected def instances = {
    instancePaths.map(s => Instance(s)).toSet
  }

  protected def createCache(): PathChildrenCache = {
    val cache = new PathChildrenCache(curator, instanceRoot, false)
    cache.start()
    cache.getListenable.addListener(this)
    cache
  }

  def childEvent(curator: CuratorFramework, event: PathChildrenCacheEvent) {
    event.getType match {
      case PathChildrenCacheEvent.Type.CHILD_ADDED |
           PathChildrenCacheEvent.Type.CHILD_REMOVED => refresh()
      case _ => log.warn("Unexpected event {}", event)
    }
  }

  def close() {
    cache.close()
    if (manuallyStarted) {
      log.info("Closing {}", curator)
      curator.close()
    }
  }

}
