/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor

import org.gridgain.scalar._
import org.gridgain.grid._
import org.scalatest._

/**
 * Test for interaction between visor and scalar.
 */
class VisorScalarSpec extends FlatSpec with Matchers {
    behavior of "A visor object"

    it should "properly open and close w/o Scalar" in {
        visor open("-d", false)
        visor status()
        visor close()
    }

    it should "properly open and close with Scalar" in {
        scalar start()

        try {
            visor open("-d", false)
            visor status()
            visor close()
        }
        finally {
            scalar stop()
        }
    }

    it should "properly open and close with named Scalar" in {
        val cfg = new GridConfiguration

        cfg.setGridName("grid-visor")

        scalar start(cfg)

        try {
            visor open("-e -g=grid-visor", false)
            visor status()
            visor close()
        }
        finally {
            scalar stop("grid-visor", true)
        }
    }

    it should "properly handle when local node stopped by Scalar" in {
        scalar start()

        try {
            visor open("-d", false)
            scalar stop()
            visor status()
        }
        finally {
            scalar stop()
        }
    }

    it should "properly open and close with Scalar & Visor mixes" in {
        scalar start()

        try {
            visor open("-d", false)
            visor status()
            visor close()

            visor open("-d", false)
            visor status()
            visor close()
        }
        finally {
            scalar stop()
        }
    }
}
