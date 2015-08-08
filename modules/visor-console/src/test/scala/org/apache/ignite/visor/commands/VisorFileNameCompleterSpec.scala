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

package org.apache.ignite.visor.commands

import org.scalatest._

import java.io.File
import java.util

/**
 * Test for visor's file name completer.
 */
class VisorFileNameCompleterSpec extends FunSpec with ShouldMatchers {
    describe("A visor file name completer") {
        it("should properly parse empty path") {
            val c = new VisorFileNameCompleter()

            val res = new util.ArrayList[CharSequence]()

            c.complete("", 0, res)

            assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)

            res.clear()

            c.complete(null, 0, res)

            assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)

            res.clear()

            c.complete("    ", 2, res)

            assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)

            res.clear()

            c.complete("help ", 5, res)

            assertResult(new File("").getAbsoluteFile.listFiles().length)(res.size)
        }
    }
}
