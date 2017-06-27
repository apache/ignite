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

/**
 * Simple POJO to store information about product
 */
public class Product {
    /** */
    private long id;

    /** */
    private String type;

    /** */
    private String title;

    /** */
    private String description;

    /** */
    private float price;

    /** */
    public Product() {
    }

    /** */
    public Product(long id, String type, String title, String description, float price) {
        this.id = id;
        this.type = type;
        this.title = title;
        this.description = description;
        this.price = price;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return ((Long)id).hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof Product && id == ((Product) obj).id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return id + ", " + price + ", " + type + ", " + title + ", " + description;
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
    public void setType(String type) {
        this.type = type;
    }

    /** */
    public String getType() {
        return type;
    }

    /** */
    public void setTitle(String title) {
        this.title = title;
    }

    /** */
    public String getTitle() {
        return title;
    }

    /** */
    public void setDescription(String description) {
        this.description = description;
    }

    /** */
    public String getDescription() {
        return description;
    }

    /** */
    public void setPrice(float price) {
        this.price = price;
    }

    /** */
    public float getPrice() {
        return price;
    }
}
