/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.tests

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import org.gridgain.scalar._
import scalar._
import org.gridgain.grid.lang._
import java.util.concurrent.atomic._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.gridgain.grid.util.lang._

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarConversionsSpec extends FlatSpec with ShouldMatchers {
    behavior of "Scalar mixin"

    it should "convert reducer" in {
        val r = new GridReducer[Int, Int] {
            var sum = 0

            override def collect(e: Int): Boolean = {
                sum += e

                true
            }

            override def reduce(): Int = {
                sum
            }
        }

        assert(r.scala.apply(Seq(1, 2, 3)) == 6)
    }

    it should "convert reducer 2" in {
        val r = new GridReducer2[Int, Int, Int] {
            var sum = 0

            override def collect(e1: Int, e2: Int): Boolean = {
                sum += e1 * e2

                true
            }

            override def apply(): Int = {
                sum
            }
        }

        assert(r.scala.apply(Seq(1, 2), Seq(3, 4)) == 21)
    }

    it should "convert reducer 3" in {
        val r = new GridReducer3[Int, Int, Int, Int] {
            var sum = 0

            override def collect(e1: Int, e2: Int, e3: Int): Boolean = {
                sum += e1 * e2 * e3

                true
            }

            override def apply(): Int = {
                sum
            }
        }

        assert(r.scala.apply(Seq(1, 2), Seq(1, 2), Seq(1, 2)) == 27)
    }

    it should "convert tuple 2" in {
        val t = new GridBiTuple[Int, Int](1, 2)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
    }

    it should "convert tuple 3" in {
        val t = new GridTuple3[Int, Int, Int](1, 2, 3)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
        assert(t.scala._3 == 3)
    }

    it should "convert tuple 4" in {
        val t = new GridTuple4[Int, Int, Int, Int](1, 2, 3, 4)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
        assert(t.scala._3 == 3)
        assert(t.scala._4 == 4)
    }

    it should "convert tuple 5" in {
        val t = new GridTuple5[Int, Int, Int, Int, Int](1, 2, 3, 4, 5)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
        assert(t.scala._3 == 3)
        assert(t.scala._4 == 4)
        assert(t.scala._5 == 5)
    }

    it should "convert in closure" in {
        val i = new AtomicInteger()

        val f = new GridInClosure[Int] {
            override def apply(e: Int) {
                i.set(e * 3)
            }
        }

        f.scala.apply(3)

        assert(i.get == 9)
    }

    it should "convert in closure 2" in {
        val i = new AtomicInteger()

        val f = new GridBiInClosure[Int, Int] {
            override def apply(e1: Int, e2: Int) {
                i.set(e1 + e2)
            }
        }

        f.scala.apply(3, 3)

        assert(i.get == 6)
    }

    it should "convert in closure 3" in {
        val i = new AtomicInteger()

        val f = new GridInClosure3[Int, Int, Int] {
            override def apply(e1: Int, e2: Int, e3: Int) {
                i.set(e1 + e2 + e3)
            }
        }

        f.scala.apply(3, 3, 3)

        assert(i.get == 9)
    }

    it should "convert absolute closure" in {
        val i = new AtomicInteger()

        val f = new GridAbsClosure {
            override def apply() {
                i.set(3)
            }
        }

        f.scala.apply()

        assert(i.get == 3)
    }

    it should "convert absolute predicate" in {
        val i = new AtomicInteger()

        val p = new GridAbsPredicate {
            override def apply(): Boolean =
                i.get > 5
        }

        i.set(5)

        assert(!p.scala.apply())

        i.set(6)

        assert(p.scala.apply())
    }

    it should "convert predicate" in {
        val p = new GridPredicate[Int] {
            override def apply(e: Int): Boolean =
                e > 5
        }

        assert(!p.scala.apply(5))
        assert(p.scala.apply(6))
    }

    it should "convert predicate 2" in {
        val p = new GridBiPredicate[Int, Int] {
            override def apply(e1: Int, e2: Int): Boolean =
                e1 + e2 > 5
        }

        assert(!p.scala.apply(2, 3))
        assert(p.scala.apply(3, 3))
    }

    it should "convert predicate 3" in {
        val p = new GridPredicate3[Int, Int, Int] {
            override def apply(e1: Int, e2: Int, e3: Int): Boolean =
                e1 + e2 + e3 > 5
        }

        assert(!p.scala.apply(1, 2, 2))
        assert(p.scala.apply(2, 2, 2))
    }

    it should "convert closure" in {
        val f = new GridClosure[Int, Int] {
            override def apply(e: Int): Int =
                e * 3
        }

        assert(f.scala.apply(3) == 9)
    }

    it should "convert closure 2" in {
        val f = new IgniteBiClosure[Int, Int, Int] {
            override def apply(e1: Int, e2: Int): Int =
                e1 + e2
        }

        assert(f.scala.apply(3, 3) == 6)
    }

    it should "convert closure 3" in {
        val f = new GridClosure3[Int, Int, Int, Int] {
            override def apply(e1: Int, e2: Int, e3: Int): Int =
                e1 + e2 + e3
        }

        assert(f.scala.apply(3, 3, 3) == 9)
    }
}
