package com.github.jond3k.hashring

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers

/**
 * These functions don't aim to prove the jvm hashcode function yields a uniform distribution, but rather catch
 * easy mistakes that can have a ruinous effect.
 *
 * @author jonathan davey <jond3k@gmail.com>
 */
class HashRingDefaultsTest extends FlatSpec with MustMatchers with HashRingHelper {

  "Default value hash entropy" must "be at least 30 buckets per 1-byte change" in {
    var last: Int = 0
    (0 to 1000).foreach(i => {
      val current = defaultValueHashFun(i.toString, 1)
      math.abs(current - last) must be >(30)
      last = current
    })
  }

  "Default value hash entropy" must "be at least 30 buckets per replica" in {
    var last: Int = 0
    (0 to 1000).foreach(i => {
      val current = defaultValueHashFun("test", i)
      math.abs(current - last) must be >(30)
      last = current
    })
  }

}
