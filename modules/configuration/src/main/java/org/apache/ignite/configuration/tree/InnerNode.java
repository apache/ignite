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

package org.apache.ignite.configuration.tree;

import java.util.NoSuchElementException;

/** */
public abstract class InnerNode implements TraversableTreeNode, Cloneable {
    /** {@inheritDoc} */
    @Override public final void accept(String key, ConfigurationVisitor visitor) {
        visitor.visitInnerNode(key, this);
    }

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre>{@code
     * @Override public void traverseChildren(ConfigurationVisitor visitor) {
     *     visitor.visitInnerNode("pojoField1", this.pojoField1);
     *
     *     visitor.visitNamedListNode("pojoField2", this.pojoField2);
     *
     *     visitor.visitLeafNode("primitiveField1", this.primitiveField1);
     *
     *     visitor.visitLeafNode("primitiveField2", this.primitiveField2);
     * }
     * }</pre>
     *
     * Order of fields must be the same as they are described in configuration schema.
     *
     * @param visitor Configuration visitor.
     */
    public abstract void traverseChildren(ConfigurationVisitor visitor);

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre>{@code
     * @Override public void traverseChild(String key, ConfigurationVisitor visitor) throws NoSuchElementException {
     *     switch (key) {
     *         case "pojoField1":
     *             visitor.visitInnerNode("pojoField1", this.pojoField1);
     *             break;
     *
     *         case "pojoField2":
     *             visitor.visitNamedListNode("pojoField2", this.pojoField2);
     *             break;
     *
     *         case "primitiveField1":
     *             visitor.visitLeafNode("primitiveField1", this.primitiveField1);
     *             break;
     *
     *         case "primitiveField2":
     *             visitor.visitLeafNode("primitiveField2", this.primitiveField2);
     *             break;
     *
     *         default:
     *             throw new NoSuchElementException(key);
     *     }
     * }
     * }</pre>
     *
     * @param key Name of the child.
     * @param visitor Configuration visitor.
     * @throws NoSuchElementException If field {@code key} is not found.
     */
    public abstract void traverseChild(String key, ConfigurationVisitor visitor) throws NoSuchElementException;

    /** {@inheritDoc} */
    @Override protected Object clone() {
        try {
            return super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
