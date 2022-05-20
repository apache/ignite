package org.apache.ignite.internal.gatling.feeder

import io.gatling.core.feeder.{Feeder, Record}

import java.util.concurrent.atomic.AtomicInteger

case class IntPairsFeeder(atomicInteger: AtomicInteger = new AtomicInteger(0)) extends Feeder[Int] {
  override def hasNext: Boolean = true

  override def next(): Record[Int] =
    Map("key" -> atomicInteger.incrementAndGet(), "value" -> atomicInteger.incrementAndGet())
}
