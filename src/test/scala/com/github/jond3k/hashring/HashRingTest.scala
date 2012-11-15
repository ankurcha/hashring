package com.github.jond3k.hashring

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers

/**
 * @author jonathan davey <jond3k@gmail.com>
 */
class HashRingTest extends FlatSpec with MustMatchers with HashRingHelper {

  it must "reject lookups if it has no values" in {
    intercept[IllegalArgumentException](ring(List.empty)(0))
  }

  it must "return V1 if V = {V1} (works with single key)" in {
    lookupsFor(11 to 13, ring(List(10)))  must equal(List(10, 10, 10))
  }

  it must "return V2 if V2 >= K > V1 (key ordering)" in {
    lookupsFor(18 to 20, ring(List(10, 20))) must equal(List(20, 20, 20))
  }

  it must "return V1 if K > V2 > V1 (keys wrap)" in {
    lookupsFor(21 to 23, ring(List(10, 20))) must equal(List(10, 10, 10))
  }

  it must "reject adding values it already has" in {
    intercept[IllegalArgumentException](ring(List(1)).add(Set(1)))
    intercept[IllegalArgumentException](ring(List(1, 2)).add(Set(2)))
  }

  it must "allow adding new values" in (ring(List(1)).add(Set(2)))

  it must "reject removing values it doesn't already have" in {
    intercept[IllegalArgumentException](ring(List()).remove(Set(1)))
    intercept[IllegalArgumentException](ring(List(2)).remove(Set(1)))
  }

  it must "allow removing existing values" in (ring(List(2)).remove(Set(2)))

  it must "return a new version that redistributes buckets for removed nodes" in {
    lookupsFor(1 to 100, ring(1 to 10).remove((1 to 5).toSet)) must equal(
      lookupsFor(1 to 100, ring(6 to 10))
    )
  }

  it must "return a new version that reallocates buckets to new nodes" in {
    lookupsFor(1 to 100, ring(1 to 5).add((6 to 10).toSet)) must equal(
      lookupsFor(1 to 100, ring(1 to 10))
    )
  }

}
