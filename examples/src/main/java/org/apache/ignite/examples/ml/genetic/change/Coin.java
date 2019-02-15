/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.ml.genetic.change;

import java.io.Serializable;

/**
 * POJO to model a coin.
 */
public class Coin implements Serializable {
    /** Define Coin Type. */
    public enum CoinType {
        /** */
        PENNY,
        /** */
        QUARTER,
        /** */
        NICKEL,
        /** */
        DIME
    }

    /** Number of coins. */
    private int numOfCoins;

    /** Coin type. */
    private CoinType coinType;

    /**
     * Create instance.
     *
     * @param coinType Type of coin.
     * @param numOfCoins Number of coins.
     */
    Coin(CoinType coinType, int numOfCoins) {
        this.coinType = coinType;
        this.numOfCoins = numOfCoins;
    }

    /**
     * Retrieve the number of coins.
     *
     * @return Number of coins.
     */
    public int getNumOfCoins() {
        return numOfCoins;
    }

    /**
     * Set the number of coins.
     *
     * @param numOfCoins Number of coins.
     */
    public void setNumOfCoins(int numOfCoins) {
        this.numOfCoins = numOfCoins;
    }

    /**
     * Retrieve Coin type.
     *
     * @return Coin type.
     */
    public CoinType getCoinType() {
        return coinType;
    }

    /**
     * Set Coin type.
     *
     * @param coinType Coin type.
     */
    public void setCoinType(CoinType coinType) {
        this.coinType = coinType;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Coin [numOfCoins=" + numOfCoins + ", coinType=" + coinType + "]";
    }
}
