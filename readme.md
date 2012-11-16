Hashring
========

A ZooKeeper-backed consistent hash ring for load balancing and partitioning, written in Scala.

Features
--------

This library will allow you to:

* Register nodes in Zookeeper
* Balance work without the need for coordination between nodes
* Provide custom hash functions so you can balance any kind of work
* Specify any number of virtual nodes per 
* Elastically grow your cluster


What is consistent hashing?
---------------------------

Hash functions provide a convenient way of sharding resources between different servers. However, if you change the size of the codomain of a normal hash function (add or remove servers), it usually means completely reassigning everything, which is particularly costly.

Consistent hash functions don't have this problem. After rebalancing, most keys will be mapped to the same values. You can find consistent hashing used in DNS, caching and databases.

Getting started
---------------

Just add the following dependency to sbt/maven

    <dependency>
      <groupId>com.github.jond3k</groupId>
      <artifactId>hashring</artifactId>
      <version>1.0</version>
    </dependency>

You can get the hashring up and running in just a few lines:

    val observer = new ClusterObserver { … }
    val cluster  = new Cluster(Instance.fromHostname(), List(observer))

This will register the ephemeral node /hashring/instance/hostname in a ZooKeeper instance localhost:2181. Whenever a new server comes online, the ClusterObserver's onRingRebalance method will be called, providing a new hashring.

You can use the hashring like this:

      if (hashRing(resource) == instance) {
        // I own this work
      }

This will allow you to uniformly distribute your resources across different instances (servers).

Usage
-----
The above example is simplistic. You ideally want to provide a ZooKeeper quorum spec and your own hash functions that map a domain-specific object (say, a user) and a server instance to the same numerical key range.

The arguments the Cluster object take are:

* ZooKeeper configuration. You can either provide configuration for the quorum, a complete NetFlix Curator object or, without any arguments, the library will create its own, connecting to localhost.
* A server identity. This is a case class that takes a unique string. If the string is used by another server it will fail. 
* A hash ring, with hash functions for instances and resources.
* One or more observers. If this is a mutable, synchronized collection you can add and remove observers. Otherwise, a regular immutable list will be fine.




