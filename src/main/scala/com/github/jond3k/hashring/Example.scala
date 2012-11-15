package com.github.jond3k.hashring

import org.apache.log4j.{Level, PatternLayout, ConsoleAppender, Logger}
import java.net.InetAddress
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry.RetryUntilElapsed

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
object Example extends App {

  class Observer(owner: Instance) extends ClusterObserver[Int] {
    def onRingRebalance(hashRing: HashRing[Int, Instance]) {
      println("%s now sees %s" format (owner, hashRing.values))
    }
  }

  // bootstrap log4j
  Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout("%5p [%t] (%F:%L) - %m%n"), "System.out"))
  Logger.getRootLogger.setLevel(Level.ERROR)
  //Logger.getLogger(classOf[Cluster[Any]]).setLevel(Level.DEBUG)

  // curator framework seems to pool connections. share one so the cluster wont try to terminate it
  val curator = CuratorFrameworkFactory.builder().connectString("localhost:2181").retryPolicy(new RetryUntilElapsed(Int.MaxValue, 1000)).namespace("hashring").build()
  curator.start()

  val clusters = (0 to 5).map(i => {
    val instance = Instance("server" + i)
    val cluster  = new Cluster(curator, instance, List(new Observer(instance)))
    Thread.sleep(2000)
    cluster
  })

  Thread.sleep(1000)
}
