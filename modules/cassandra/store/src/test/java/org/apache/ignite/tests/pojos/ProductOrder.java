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

package org.apache.ignite.tests.pojos;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Simple POJO to store information about product order
 */
public class ProductOrder {
    /** */
    private static final DateFormat FORMAT = new SimpleDateFormat("MM/dd/yyyy/S");

    /** */
    private static final DateFormat FULL_FORMAT = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss:S");

    /** */
    private long id;

    /** */
    private long productId;

    /** */
    private Date date;

    /** */
    private int amount;

    /** */
    private float price;

    /** */
    public ProductOrder() {
    }

    /** */
    public ProductOrder(long id, Product product, Date date, int amount) {
        this(id, product.getId(), product.getPrice(), date, amount);
    }

    /** */
    public ProductOrder(long id, long productId, float productPrice, Date date, int amount) {
        this.id = id;
        this.productId = productId;
        this.date = date;
        this.amount = amount;
        this.price = productPrice * amount;

        // if user ordered more than 10 items provide 5% discount
        if (amount > 10)
            price *= 0.95F;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return ((Long)id).hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof ProductOrder && id == ((ProductOrder) obj).id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return id + ", " + productId + ", " + FULL_FORMAT.format(date) + ", " + getDayMillisecond() + ", " + amount + ", " + price;
    }

    /** */
    public void setId(long id) {
        this.id = id;
    }

    /** */
    public long getId() {
        return id;
    }

    /** */
    public void setProductId(long productId) {
        this.productId = productId;
    }

    /** */
    public long getProductId() {
        return productId;
    }

    /** */
    public void setDate(Date date) {
        this.date = date;
    }

    /** */
    public Date getDate() {
        return date;
    }

    /** */
    public void setAmount(int amount) {
        this.amount = amount;
    }

    /** */
    public int getAmount() {
        return amount;
    }

    /** */
    public void setPrice(float price) {
        this.price = price;
    }

    /** */
    public float getPrice() {
        return price;
    }

    /** */
    public String getDayMillisecond() {
        return FORMAT.format(date);
    }
}
