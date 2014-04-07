/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.scalar.tests.examples

/**
 * Scalar examples multi-node self test.
 */
class ScalarExamplesMultiNodeSelfTest extends ScalarExamplesSelfTest {
    /** */
    protected override def beforeTest() {
        startRemoteNodes()
    }

    /** */
    protected override def getTestTimeout: Long = {
        10 * 60 * 1000
    }
}
