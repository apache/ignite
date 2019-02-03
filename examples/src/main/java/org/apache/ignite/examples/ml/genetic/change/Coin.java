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
