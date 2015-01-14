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

package org.gridgain.scalar.tests

import org.scalatest.matchers._
import org.gridgain.scalar._
import scalar._
import org.scalatest._
import junit.JUnitRunner
import scala.util.control.Breaks._
import org.junit.runner.RunWith

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarReturnableSpec extends FlatSpec with ShouldMatchers {
    "Scalar '^^'" should "work" in {
        var i = 0

        breakable {
            while (true) {
                if (i == 0)
                    println("Only once!") ^^

                i += 1
            }
        }

        assert(i == 0)
    }

    "Scalar '^^'" should "also work" in {
        test()
    }

    // Ignore exception below.
    def test() = breakable {
        while (true) {
            println("Only once!") ^^
        }
    }
}
