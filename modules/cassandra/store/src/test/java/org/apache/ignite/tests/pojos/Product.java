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
