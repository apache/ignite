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
public abstract class InnerNode implements TraversableTreeNode, ConstructableTreeNode, Cloneable {
    /** {@inheritDoc} */
    @Override public final <T> T accept(String key, ConfigurationVisitor<T> visitor) {
        return visitor.visitInnerNode(key, this);
    }

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre><code>
     * {@literal @}Override public void traverseChildren(ConfigurationVisitor visitor) {
     *     visitor.visitInnerNode("pojoField1", this.pojoField1);
     *
     *     visitor.visitNamedListNode("pojoField2", this.pojoField2);
     *
     *     visitor.visitLeafNode("primitiveField1", this.primitiveField1);
     *
     *     visitor.visitLeafNode("primitiveField2", this.primitiveField2);
     * }
     * </code></pre>
     *
     * Order of fields must be the same as they are described in configuration schema.
     *
     * @param visitor Configuration visitor.
     */
    public abstract <T> void traverseChildren(ConfigurationVisitor<T> visitor);

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre><code>
     * {@literal @}Override public void traverseChild(String key, ConfigurationVisitor visitor) throws NoSuchElementException {
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
     * </code></pre>
     *
     * @param key Name of the child.
     * @param visitor Configuration visitor.
     * @throws NoSuchElementException If field {@code key} is not found.
     */
    public abstract <T> T traverseChild(String key, ConfigurationVisitor<T> visitor) throws NoSuchElementException;

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre><code>
     * {@literal @}Override public abstract void construct(String key, ConfigurationSource src) throws NoSuchElementException {
     *     switch (key) {
     *         case "namedList":
     *             if (src == null)
     *                 namedList = new{@code NamedListNode<>}(Foo::new);
     *             else
     *                 src.descend(namedList = namedList.copy());
     *             break;
     *
     *         case "innerNode":
     *             if (src == null)
     *                 innerNode = null;
     *             else
     *                 src.descend(innerNode = (innerNode == null ? new Bar() : (Bar)innerNode.copy()));
     *             break;
     *
     *         case "leaf":
     *             leaf = src == null ? null : src.unwrap(Integer.class);
     *             break;
     *
     *         default: throw new NoSuchElementException(key);
     *     }
     * }
     * </code></pre>
     * {@inheritDoc}
     */
    @Override public abstract void construct(String key, ConfigurationSource src) throws NoSuchElementException;

    /**
     * Assigns default value to the corresponding leaf. Defaults are gathered from configuration schema class.
     *
     * @param fieldName Field name to be initialized.
     * @return {@code true} if default value has been assigned, {@code false} otherwise.
     * @throws NoSuchElementException If there's no such field or it is not a leaf value.
     */
    public abstract boolean constructDefault(String fieldName) throws NoSuchElementException;

    /**
     * @return Class of corresponding configuration schema.
     */
    public abstract Class<?> schemaType();

    /** {@inheritDoc} */
    @Override public InnerNode copy() {
        try {
            return (InnerNode)clone();
        }
        catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
