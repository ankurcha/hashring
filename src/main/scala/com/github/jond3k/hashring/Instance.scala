package com.github.jond3k.hashring

import java.net.InetAddress

/**
 * Uniquely identifies a server in the cluster
 */
case class Instance(id: String)

object Instance {
  /**
   * Create an instance identified by the localhost's hostname. Easy to use but inherently problematic
   */
  def fromHostname(): Instance = {
    Instance(InetAddress.getLocalHost.getHostName)
  }
}