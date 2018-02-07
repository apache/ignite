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

package org.apache.ignite.scalar.examples

import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.util.Random
import scala.util.control.Breaks._

/**
 * Scalar-based Monte-Carlo example.
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCreditRiskExample {
    def main(args: Array[String]) {
        scalar("examples/config/example-ignite.xml") {
            // Create portfolio.
            var portfolio = Seq.empty[Credit]

            val rnd = new Random

            // Generate some test portfolio items.
            (0 until 5000).foreach(i =>
                portfolio +:= Credit(
                    50000 * rnd.nextDouble,
                    rnd.nextInt(1000),
                    rnd.nextDouble / 10,
                    rnd.nextDouble / 20 + 0.02
                )
            )

            // Forecast horizon in days.
            val horizon = 365

            // Number of Monte-Carlo iterations.
            val iter = 10000

            // Percentile.
            val percentile = 0.95

            // Mark the stopwatch.
            val start = System.currentTimeMillis

            // Calculate credit risk and print it out.
            // As you can see the ignite cluster enabling is completely hidden from the caller
            // and it is fully transparent to him. In fact, the caller is never directly
            // aware if method was executed just locally or on the 100s of cluster nodes.
            // Credit risk crdRisk is the minimal amount that creditor has to have
            // available to cover possible defaults.
            val crdRisk = ignite$ @< (closures(ignite$.cluster().nodes().size(), portfolio.toArray, horizon, iter, percentile),
                (s: Seq[Double]) => s.sum / s.size, null)

            println("Credit risk [crdRisk=" + crdRisk + ", duration=" +
                (System.currentTimeMillis - start) + "ms]")
        }
    }

    /**
     * Creates closures for calculating credit risks.
     *
     * @param clusterSize Size of the cluster.
     * @param portfolio Portfolio.
     * @param horizon Forecast horizon in days.
     * @param iter Number of Monte-Carlo iterations.
     * @param percentile Percentile.
     * @return Collection of closures.
     */
    private def closures(clusterSize: Int, portfolio: Array[Credit], horizon: Int, iter: Int,
        percentile: Double): Seq[() => Double] = {
        val iterPerNode: Int = math.round(iter / clusterSize.asInstanceOf[Float])
        val lastNodeIter: Int = iter - (clusterSize - 1) * iterPerNode

        var cls = Seq.empty[() => Double]

        (0 until clusterSize).foreach(i => {
            val nodeIter = if (i == clusterSize - 1) lastNodeIter else iterPerNode

            cls +:= (() => new CreditRiskManager().calculateCreditRiskMonteCarlo(
                portfolio, horizon, nodeIter, percentile))
        })

        cls
    }
}

/**
 * This class provides a simple model for a credit contract (or a loan). It is basically
 * defines as remaining crediting amount to date, credit remaining term, APR and annual
 * probability on default. Although this model is simplified for the purpose
 * of this example, it is close enough to emulate the real-life credit
 * risk assessment application.
 */
private case class Credit(
    remAmnt: Double, // Remaining crediting amount.
    remTerm: Int,    // Remaining crediting remTerm.
    apr: Double,     // Annual percentage rate (APR).
    edf: Double      // Expected annual probability of default (EaDF).
) {
    /**
     * Gets either credit probability of default for the given period of time
     * if remaining term is less than crediting time or probability of default
     * for whole remained crediting time.
     *
     * @param term Default term.
     * @return Credit probability of default in relative percents
     *     (percentage / 100).
     */
    def getDefaultProbability(term: Int): Double = {
        (1 - math.exp(math.log(1 - edf) * math.min(remTerm, term) / 365.0))
    }
}

/**
 * This class abstracts out the calculation of risk for a credit portfolio.
 */
private class CreditRiskManager {
    /**
     * Default randomizer with normal distribution.
     * Note that since every JVM on the ignite cluster will have its own random
     * generator (independently initialized) the Monte-Carlo simulation
     * will be slightly skewed when performed on the ignite cluster due to skewed
     * normal distribution of the sub-jobs comparing to execution on the
     * local node only with single random generator. Real-life applications
     * may want to provide its own implementation of distributed random
     * generator.
     */
    private val rndGen = new Random

    /**
     * Calculates credit risk for a given credit portfolio. This calculation uses
     * Monte-Carlo Simulation to produce risk value.
     *
     * @param portfolio Credit portfolio.
     * @param horizon Forecast horizon (in days).
     * @param num Number of Monte-Carlo iterations.
     * @param percentile Cutoff level.
     * @return Credit risk value, i.e. the minimal amount that creditor has to
     *      have available to cover possible defaults.
     */
    def calculateCreditRiskMonteCarlo(portfolio: Seq[Credit], horizon: Int, num:
        Int, percentile: Double): Double = {
        println(">>> Calculating credit risk for portfolio [size=" + portfolio.length + ", horizon=" +
            horizon + ", percentile=" + percentile + ", iterations=" + num + "] <<<")

        val start = System.currentTimeMillis

        val losses = calculateLosses(portfolio, horizon, num).sorted
        val lossProbs = new Array[Double](losses.size)

        (0 until losses.size).foreach(i => {
            if (i == 0)
                lossProbs(i) = getLossProbability(losses, 0)
            else if (losses(i) != losses(i - 1))
                lossProbs(i) = getLossProbability(losses, i) + lossProbs(i - 1)
            else
                lossProbs(i) = lossProbs(i - 1)
        })

        var crdRisk = 0.0

        breakable {
            (0 until lossProbs.size).foreach(i => {
                if (lossProbs(i) > percentile) {
                    crdRisk = losses(i - 1)

                    break()
                }
            })
        }

        println(">>> Finished calculating portfolio risk [risk=" + crdRisk +
            ", time=" + (System.currentTimeMillis - start) + "ms]")

        crdRisk
    }

    /**
     * Calculates losses for the given credit portfolio using Monte-Carlo Simulation.
     * Simulates probability of default only.
     *
     * @param portfolio Credit portfolio.
     * @param horizon Forecast horizon.
     * @param num Number of Monte-Carlo iterations.
     * @return Losses array simulated by Monte Carlo method.
     */
    private def calculateLosses(portfolio: Seq[Credit], horizon: Int, num: Int): Array[Double] = {
        val losses = new Array[Double](num)

        // Count losses using Monte-Carlo method. We generate random probability of default,
        // if it exceeds certain credit default value we count losses - otherwise count income.
        (0 until num).foreach(i => {
            portfolio.foreach(crd => {
                val remDays = math.min(crd.remTerm, horizon)

                if (rndGen.nextDouble >= 1 - crd.getDefaultProbability(remDays))
                    // (1 + 'r' * min(H, W) / 365) * S.
                    // Where W is a horizon, H is a remaining crediting term, 'r' is an annual credit rate,
                    // S is a remaining credit amount.
                    losses(i) += (1 + crd.apr * math.min(horizon, crd.remTerm) / 365) * crd.remAmnt
                else
                    // - 'r' * min(H,W) / 365 * S
                    // Where W is a horizon, H is a remaining crediting term, 'r' is a annual credit rate,
                    // S is a remaining credit amount.
                    losses(i) -= crd.apr * math.min(horizon, crd.remTerm) / 365 * crd.remAmnt
            })
        })

        losses
    }

    /**
     * Calculates probability of certain loss in array of losses.
     *
     * @param losses Array of losses.
     * @param i Index of certain loss in array.
     * @return Probability of loss with given index.
     */
    private def getLossProbability(losses: Array[Double], i: Int): Double = {
        var count = 0.0

        losses.foreach(tmp => {
            if (tmp == losses(i))
                count += 1
        })

        count / losses.size
    }
}
