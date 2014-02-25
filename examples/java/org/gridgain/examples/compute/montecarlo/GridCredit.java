// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute.montecarlo;

import java.io.*;

/**
 * This class provides a simple model for a credit contract (or a loan). It is basically
 * defines as remaining crediting amount to date, credit remaining term, APR and annual
 * probability on default. Although this model is simplified for the purpose
 * of this example, it is close enough to emulate the real-life credit
 * risk assessment application.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCredit implements Serializable {
    /** Remaining crediting amount. */
    private final double remAmnt;

    /** Remaining crediting remTerm. */
    private final int remTerm;

    /** Annual percentage rate (APR). */
    private final double apr;

    /** Expected annual probability of default (EaDF). */
    private final double edf;

    /**
     * Creates new credit instance with given information.
     *
     * @param remAmnt Remained crediting amount.
     * @param remTerm Remained crediting remTerm.
     * @param apr Annual percentage rate (APR).
     * @param edf Expected annual probability of default (EaDF).
     */
    public GridCredit(double remAmnt, int remTerm, double apr, double edf) {
        this.remAmnt = remAmnt;
        this.remTerm = remTerm;
        this.apr = apr;
        this.edf = edf;
    }

    /**
     * Gets remained crediting amount.
     *
     * @return Remained amount of credit.
     */
    double getRemainingAmount() {
        return remAmnt;
    }

    /**
     * Gets remained crediting remTerm.
     *
     * @return Remained crediting remTerm in days.
     */
    int getRemainingTerm() {
        return remTerm;
    }

    /**
     * Gets annual percentage rate.
     *
     * @return Annual percentage rate in relative percents (percentage / 100).
     */
    double getAnnualRate() {
        return apr;
    }

    /**
     * Gets either credit probability of default for the given period of time
     * if remaining term is less than crediting time or probability of default
     * for whole remained crediting time.
     *
     * @param term Default term.
     * @return Credit probability of default in relative percents
     *     (percentage / 100).
     */
    double getDefaultProbability(int term) {
        return 1 - Math.exp(Math.log(1 - edf) * Math.min(remTerm, term) / 365.0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getName());
        buf.append(" [remAmnt=").append(remAmnt);
        buf.append(", remTerm=").append(remTerm);
        buf.append(", apr=").append(apr);
        buf.append(", edf=").append(edf);
        buf.append(']');

        return buf.toString();
    }
}
