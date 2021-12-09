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

package org.apache.ignite.internal.configuration.tree;

import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.InternalId;

/**
 * Configuration node implementation.
 */
public abstract class InnerNode implements TraversableTreeNode, ConstructableTreeNode, Cloneable {
    /** Configuration field name with {@link InternalId}. */
    public static final String INTERNAL_ID = "<internal_id>";

    /** Configuration field name with {@link InjectedName}. */
    public static final String INJECTED_NAME = "<injected_name>";

    /**
     * Internal id of the node. May be {@code null}.
     */
    private UUID internalId;

    /**
     * Generates new internal id for the node.
     *
     * @return Generated internal id.
     */
    public UUID generateInternalId() {
        return internalId = UUID.randomUUID();
    }

    /**
     * Returns internal id of the node.
     *
     * @return Internal id of the node.
     */
    public final UUID internalId() {
        return internalId;
    }

    /**
     * Sets a new internal id value to the node.
     *
     * @param internalId New internal id value.
     */
    public final void internalId(UUID internalId) {
        this.internalId = internalId;
    }

    /** {@inheritDoc} */
    @Override
    public final <T> T accept(String key, ConfigurationVisitor<T> visitor) {
        return visitor.visitInnerNode(key, this);
    }

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre><code>
     * {@literal @}Override public &lt;T&gt; void traverseChildren(ConfigurationVisitor visitor, boolean includeInternal) {
     *     visitor.visitInnerNode("pojoField1", this.pojoField1);
     *
     *     visitor.visitNamedListNode("pojoField2", this.pojoField2);
     *
     *     visitor.visitLeafNode("primitiveField1", this.primitiveField1);
     *
     *     if (includeInternal) {
     *          visitor.visitLeafNode("primitiveField2", this.primitiveField2);
     *     }
     * }
     * </code></pre>
     *
     * <p>Order of fields must be the same as they are described in configuration schema.
     *
     * @param visitor         Configuration visitor.
     * @param includeInternal Include internal configuration nodes (private configuration extensions).
     * @param <T>             Parameter type of the passed visitor.
     */
    public abstract <T> void traverseChildren(ConfigurationVisitor<T> visitor, boolean includeInternal);

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre><code>
     * {@literal @}Override public &lt;T&gt; T traverseChild(String key, ConfigurationVisitor visitor, boolean includeInternal) throws
     *     NoSuchElementException {
     *     if (includeInternal) {
     *         switch (key) {
     *             case "pojoField1":
     *                 return visitor.visitInnerNode("pojoField1", this.pojoField1);
     *
     *             case "pojoField2":
     *                 return visitor.visitNamedListNode("pojoField2", this.pojoField2);
     *
     *             case "primitiveField1":
     *                 return visitor.visitLeafNode("primitiveField1", this.primitiveField1);
     *
     *             case "primitiveField2":
     *                 return visitor.visitLeafNode("primitiveField2", this.primitiveField2);
     *
     *             default:
     *                 throw new NoSuchElementException(key);
     *         }
     *     }
     *     else {
     *         switch (key) {
     *             case "primitiveField2":
     *                  return visitor.visitLeafNode("primitiveField2", this.primitiveField2);
     *             default:
     *                  throw new NoSuchElementException(key);
     *         }
     *     }
     * }
     * </code></pre>
     *
     * @param key             Name of the child.
     * @param visitor         Configuration visitor.
     * @param includeInternal Include internal configuration nodes (private configuration extensions).
     * @param <T>             Parameter type of passed visitor.
     * @return Whatever {@code visitor} returned.
     * @throws NoSuchElementException If field {@code key} is not found.
     */
    public abstract <T> T traverseChild(
            String key,
            ConfigurationVisitor<T> visitor,
            boolean includeInternal
    ) throws NoSuchElementException;

    /**
     * Method with auto-generated implementation. Must look like this:
     * <pre><code>
     * {@literal @}Override public abstract void construct(String key, ConfigurationSource src, boolean includeInternal)
     *     throws NoSuchElementException {
     *     if (includeInternal) {
     *         switch (key) {
     *              case "namedList":
     *                  if (src == null)
     *                      namedList = new NamedListNode&lt;&gt;(Foo::new);
     *                  else
     *                      src.descend(namedList = namedList.copy());
     *                  break;
     *
     *              case "innerNode":
     *                  if (src == null)
     *                      innerNode = null;
     *                  else
     *                      src.descend(innerNode = (innerNode == null ? new Bar() : (Bar)innerNode.copy()));
     *                  break;
     *
     *              case "leafInt":
     *                  leafInt = src == null ? null : src.unwrap(Integer.class);
     *                  break;
     *
     *              case "leafStr":
     *                  leafStr = src == null ? null : src.unwrap(String.class);
     *                  break;
     *
     *              default: throw new NoSuchElementException(key);
     *         }
     *     }
     *     switch (key) {
     *         switch (key) {
     *              case "leafStr":
     *                  leafStr = src == null ? null : src.unwrap(String.class);
     *                  break;
     *
     *              default: throw new NoSuchElementException(key);
     *         }
     *     }
     * }
     * </code></pre>
     * {@inheritDoc}
     */
    @Override
    public abstract void construct(
            String key,
            ConfigurationSource src,
            boolean includeInternal
    ) throws NoSuchElementException;

    /**
     * Assigns default value to the corresponding leaf. Defaults are gathered from configuration schema class.
     *
     * @param fieldName Field name to be initialized.
     * @throws NoSuchElementException If there's no such field or it is not a leaf value.
     */
    public abstract void constructDefault(String fieldName) throws NoSuchElementException;

    /**
     * Returns class of corresponding configuration schema.
     *
     * @return Class of corresponding configuration schema.
     */
    public abstract Class<?> schemaType();

    /** {@inheritDoc} */
    @Override
    public InnerNode copy() {
        try {
            return (InnerNode) clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns specific {@code Node} of the value. Overridden for polymorphic configuration to get a specific polymorphic configuration
     * instance.
     *
     * @param <NODET> Type of the {@code Node}.
     * @return Specific {@code Node} of the value.
     */
    public <NODET> NODET specificNode() {
        return (NODET) this;
    }

    /**
     * Returns the value of a field with {@link InjectedName}.
     */
    public String getInjectedNameFieldValue() {
        return null;
    }

    /**
     * Sets the value of a field with {@link InjectedName}.
     */
    public void setInjectedNameFieldValue(String value) {
        // No-op.
    }
}
