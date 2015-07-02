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

package org.apache.ignite.visor.commands.start

import org.apache.ignite.visor.visor
import org.scalatest._

import org.apache.ignite.visor.commands.open.VisorOpenCommand._
import org.apache.ignite.visor.commands.start.VisorStartCommand._
import org.apache.ignite.visor.commands.top.VisorTopologyCommand._

/**
 * Unit test for 'start' command.
 */
class VisorStartCommandSpec extends FunSpec with Matchers with BeforeAndAfterAll {
    override def beforeAll() {
        visor.open("-d")
    }

    override def afterAll() {
        visor.close()
    }

    describe("A 'start' visor command") {
        it("should should start one new node") {
            visor.start("-h=192.168.1.103 -r -p=password")
        }

        it("should should start two nodes") {
            visor.start("-h=uname:passwd@localhost -n=2")
        }

        it("should print error message with invalid port number") {
            visor.start("-h=localhost:x -p=passwd")
        }

        it("should print error message with zero port number") {
            visor.start("-h=localhost:0 -p=passwd")
        }

        it("should print error message with negative port number") {
            visor.start("-h=localhost:-1 -p=passwd")
        }

        it("should print error message with invalid nodes count") {
            visor.start("-h=localhost#x -p=passwd")
        }

        it("should print error message with zero nodes count") {
            visor.start("-h=localhost#0 -p=passwd")
        }

        it("should print error message with negative nodes count") {
            visor.start("-h=localhost#-1 -p=passwd")
        }

        it("should print error message with incorrect host") {
            visor.start("-h=incorrect -p=passwd")
        }

        it("should print error message with incorrect username") {
            visor.start("-h=incorrect@localhost -p=passwd")
        }

        it("should print error message with incorrect password") {
            visor.start("-h=uname:incorrect@localhost")
        }

        it("should print error message with nonexistent script path") {
            visor.start("-h=uname:passwd@localhost -s=incorrect")
        }

        it("should print error message with incorrect script path") {
            visor.start("-h=uname:passwd@localhost -s=bin/readme.txt")
        }

        it("should print error message with nonexistent config path") {
            visor.start("-h=uname:passwd@localhost -c=incorrect")
        }

        it("should print error message with incorrect config path") {
            visor.start("-h=uname:passwd@localhost -c=bin/readme.txt")
        }

        it("should start one node") {
            visor.start("-h=uname:passwd@localhost")

            visor.top()
        }

        it("should start one node on host identified by IP") {
            visor.start("-h=uname:passwd@127.0.0.1")

            visor.top()
        }

        it("should start two nodes") {
            visor.start("-h=uname:passwd@localhost#2")

            visor.top()
        }

        it("should restart 4 nodes") {
            visor.start("-h=uname:passwd@localhost#4 -r")

            visor.top()
        }
    }
}
