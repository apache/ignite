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
 * POJO to model a coin
 */
public class Coin implements Serializable {
    /** Define Coin Type */
    public static enum CoinType {
        PENNY, QUARTER, NICKEL, DIME
    }

    ;

    /** number of coins */
    private int numberOfCoins = 0;

    /** CoinType */
    private CoinType coinType = null;

    /**
     * @param coinType Type of coin
     * @param numberOfCoins Number of coins
     */
    public Coin(CoinType coinType, int numberOfCoins) {
        this.coinType = coinType;
        this.numberOfCoins = numberOfCoins;
    }

    /**
     * Retrieve the number of coins
     *
     * @return Number of coins
     */
    public int getNumberOfCoins() {
        return numberOfCoins;
    }

    /**
     * Set the number of coins
     *
     * @param numberOfCoins Number of coins
     */
    public void setNumberOfCoins(int numberOfCoins) {
        this.numberOfCoins = numberOfCoins;
    }

    /**
     * Retrieve Coin type
     *
     * @return Coin type
     */
    public CoinType getCoinType() {
        return coinType;
    }

    /**
     * Set Coin type
     *
     * @param coinType Coin Type
     */
    public void setCoinType(CoinType coinType) {
        this.coinType = coinType;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Coin [numberOfCoins=" + numberOfCoins + ", coinType=" + coinType + "]";
    }

}
