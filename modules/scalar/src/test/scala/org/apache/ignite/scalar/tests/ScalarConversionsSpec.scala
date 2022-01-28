/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.scalar.tests

import org.apache.ignite.internal.util.lang._
import org.apache.ignite.lang._
import org.apache.ignite.scalar.scalar._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.ShouldMatchers

import java.util.concurrent.atomic._

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarConversionsSpec extends FunSpec with ShouldMatchers {
    describe("Scalar mixin") {

    it("should convert reducer") {
        val r = new IgniteReducer[Int, Int] {
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

    it("should convert reducer 2") {
        val r = new IgniteReducer2[Int, Int, Int] {
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

    it("should convert reducer 3") {
        val r = new IgniteReducer3[Int, Int, Int, Int] {
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

    it("should convert tuple 2") {
        val t = new IgniteBiTuple[Int, Int](1, 2)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
    }

    it("should convert tuple 3") {
        val t = new GridTuple3[Int, Int, Int](1, 2, 3)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
        assert(t.scala._3 == 3)
    }

    it("should convert tuple 4") {
        val t = new GridTuple4[Int, Int, Int, Int](1, 2, 3, 4)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
        assert(t.scala._3 == 3)
        assert(t.scala._4 == 4)
    }

    it("should convert tuple 5") {
        val t = new GridTuple5[Int, Int, Int, Int, Int](1, 2, 3, 4, 5)

        assert(t.scala._1 == 1)
        assert(t.scala._2 == 2)
        assert(t.scala._3 == 3)
        assert(t.scala._4 == 4)
        assert(t.scala._5 == 5)
    }

    it("should convert in closure") {
        val i = new AtomicInteger()

        val f = new IgniteInClosure[Int] {
            override def apply(e: Int) {
                i.set(e * 3)
            }
        }

        f.scala.apply(3)

        assert(i.get == 9)
    }

    it("should convert in closure 2") {
        val i = new AtomicInteger()

        val f = new IgniteBiInClosure[Int, Int] {
            override def apply(e1: Int, e2: Int) {
                i.set(e1 + e2)
            }
        }

        f.scala.apply(3, 3)

        assert(i.get == 6)
    }

    it("should convert in closure 3") {
        val i = new AtomicInteger()

        val f = new GridInClosure3[Int, Int, Int] {
            override def apply(e1: Int, e2: Int, e3: Int) {
                i.set(e1 + e2 + e3)
            }
        }

        f.scala.apply(3, 3, 3)

        assert(i.get == 9)
    }

    it("should convert absolute closure") {
        val i = new AtomicInteger()

        val f = new GridAbsClosure {
            override def apply() {
                i.set(3)
            }
        }

        f.scala.apply()

        assert(i.get == 3)
    }

    it("should convert absolute predicate") {
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

    it("should convert predicate") {
        val p = new IgnitePredicate[Int] {
            override def apply(e: Int): Boolean =
                e > 5
        }

        assert(!p.scala.apply(5))
        assert(p.scala.apply(6))
    }

    it("should convert predicate 2") {
        val p = new IgniteBiPredicate[Int, Int] {
            override def apply(e1: Int, e2: Int): Boolean =
                e1 + e2 > 5
        }

        assert(!p.scala.apply(2, 3))
        assert(p.scala.apply(3, 3))
    }

    it("should convert predicate 3") {
        val p = new GridPredicate3[Int, Int, Int] {
            override def apply(e1: Int, e2: Int, e3: Int): Boolean =
                e1 + e2 + e3 > 5
        }

        assert(!p.scala.apply(1, 2, 2))
        assert(p.scala.apply(2, 2, 2))
    }

    it("should convert closure") {
        val f = new IgniteClosure[Int, Int] {
            override def apply(e: Int): Int =
                e * 3
        }

        assert(f.scala.apply(3) == 9)
    }

    it("should convert closure 2") {
        val f = new IgniteBiClosure[Int, Int, Int] {
            override def apply(e1: Int, e2: Int): Int =
                e1 + e2
        }

        assert(f.scala.apply(3, 3) == 6)
    }

    it("should convert closure 3") {
        val f = new GridClosure3[Int, Int, Int, Int] {
            override def apply(e1: Int, e2: Int, e3: Int): Int =
                e1 + e2 + e3
        }

        assert(f.scala.apply(3, 3, 3) == 9)
    }
    }
}
