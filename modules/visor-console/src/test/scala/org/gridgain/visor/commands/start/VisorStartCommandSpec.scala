/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.start

import org.scalatest._

import org.gridgain.visor._
import org.gridgain.visor.commands.start.VisorStartCommand._
import org.gridgain.visor.commands.top.VisorTopologyCommand._

/**
 * Unit test for 'start' command.
 */
class VisorStartCommandSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    override def beforeAll() {
        visor.open("-d")
    }

    override def afterAll() {
        visor.close()
    }

    behavior of "A 'start' visor command"

    it should "should start one new node" in {
        visor.start("-h=192.168.1.103 -r -p=password")
    }

    it should "should start two nodes" in {
        visor.start("-h=uname:passwd@localhost -n=2")
    }

    it should "print error message with invalid port number" in {
        visor.start("-h=localhost:x -p=passwd")
    }

    it should "print error message with zero port number" in {
        visor.start("-h=localhost:0 -p=passwd")
    }

    it should "print error message with negative port number" in {
        visor.start("-h=localhost:-1 -p=passwd")
    }

    it should "print error message with invalid nodes count" in {
        visor.start("-h=localhost#x -p=passwd")
    }

    it should "print error message with zero nodes count" in {
        visor.start("-h=localhost#0 -p=passwd")
    }

    it should "print error message with negative nodes count" in {
        visor.start("-h=localhost#-1 -p=passwd")
    }

    it should "print error message with incorrect host" in {
        visor.start("-h=incorrect -p=passwd")
    }

    it should "print error message with incorrect username" in {
        visor.start("-h=incorrect@localhost -p=passwd")
    }

    it should "print error message with incorrect password" in {
        visor.start("-h=uname:incorrect@localhost")
    }

    it should "print error message with nonexistent script path" in {
        visor.start("-h=uname:passwd@localhost -s=incorrect")
    }

    it should "print error message with incorrect script path" in {
        visor.start("-h=uname:passwd@localhost -s=bin/readme.txt")
    }

    it should "print error message with nonexistent config path" in {
        visor.start("-h=uname:passwd@localhost -c=incorrect")
    }

    it should "print error message with incorrect config path" in {
        visor.start("-h=uname:passwd@localhost -c=bin/readme.txt")
    }

    it should "start one node" in {
        visor.start("-h=uname:passwd@localhost")

        visor.top()
    }

    it should "start one node on host identified by IP" in {
        visor.start("-h=uname:passwd@127.0.0.1")

        visor.top()
    }

    it should "start two nodes" in {
        visor.start("-h=uname:passwd@localhost#2")

        visor.top()
    }

    it should "restart 4 nodes" in {
        visor.start("-h=uname:passwd@localhost#4 -r")

        visor.top()
    }
}
