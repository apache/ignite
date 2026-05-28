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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a set of Ignite node features. A feature set determines the behavior of an Ignite node. Therefore,
 * comparing the feature sets of Ignite nodes running different versions helps identify behavioral differences between
 * versions and determine Rolling Upgrade compatibility.
 *
 * <p>The Ignite Feature Set divides all features into two categories: features that can be deactivated to simulate
 * the behavior of nodes from previous versions, and features that cannot be deactivated.</p>
 *
 * <p>Note that the Ignite Feature Set operates on {@link IgniteFeature} identifiers and relies on the following
 * properties of {@link IgniteFeature} identifiers:</p>
 *
 * <ul>
 *     <li>The identifiers of existing features do not change between Ignite versions</li>
 *     <li>Identifier values start at {@code 0} and increase monotonically</li>
 * </ul>
 *
 * <p>The feature set divides features into three ranges:</p>
 *
 * <ol>
 *     <li>
 *         A continuous range of feature IDs that cannot be deactivated:
 *         {@code [0 -> }{@link IgniteFeatureSet#rangeStartInclusive}{@code )}
 *     </li>
 *     <li>
 *         A continuous range of feature IDs that can be deactivated:
 *         {@code [}{@link IgniteFeatureSet#rangeStartInclusive}{@code -> }
 *         {@link IgniteFeatureSet#rangeEndInclusive}{@code ]}
 *     </li>
 *     <li>
 *         A sparse suffix containing IDs of features that can be deactivated. The identifier values in this range
 *         are greater than {@link IgniteFeatureSet#rangeEndInclusive}
 *     </li>
 * </ol>
 *
 * @see IgniteFeature
 */
public class IgniteFeatureSet implements Iterable<Integer>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int rangeStartInclusive;

    /** */
    private int rangeEndInclusive;

    /** */
    @Nullable private GridIntList sparseSuffix;

    /** */
    public IgniteFeatureSet() {
        // No-op.
    }

    /** */
    private IgniteFeatureSet(int rangeStartInclusive, int rangeEndInclusive, @Nullable GridIntList sparseSuffix) {
        A.ensure(rangeStartInclusive >= 0, "rangeStart must be greater than or equal to 0");
        A.ensure(rangeEndInclusive >= rangeStartInclusive, "rangeEnd must be greater than or equal to rangeStart");

        this.rangeStartInclusive = rangeStartInclusive;
        this.rangeEndInclusive = rangeEndInclusive;
        this.sparseSuffix = sparseSuffix;
    }

    /** */
    public boolean isUpgradableTo(IgniteFeatureSet target) {
        GridIntList diff = difference(target);

        for (GridIntIterator iter = diff.iterator(); iter.hasNext(); ) {
            int featureId = iter.next();

            if (!target.contains(featureId) || !target.canDeactivate(featureId))
                return false;
        }

        return true;
    }

    /** */
    public GridIntList difference(IgniteFeatureSet other) {
        GridIntList res = new GridIntList();

        for (int featureId : other) {
            if (!contains(featureId))
                res.add(featureId);
        }

        for (int featureId : this) {
            if (!other.contains(featureId))
                res.add(featureId);
        }

        return res;
    }

    /** */
    public boolean contains(int featureId) {
        if (featureId <= rangeEndInclusive)
            return true;

        return sparseSuffix != null && sparseSuffix.contains(featureId);
    }

    /** */
    private int calculateSize() {
        int size = rangeEndInclusive + 1;

        if (sparseSuffix != null)
            size += sparseSuffix.size();

        return size;
    }

    /** */
    private boolean canDeactivate(int featureId) {
        return rangeStartInclusive <= featureId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IgniteFeatureSet that = (IgniteFeatureSet)o;

        return rangeStartInclusive == that.rangeStartInclusive
            && rangeEndInclusive == that.rangeEndInclusive
            && Objects.equals(sparseSuffix, that.sparseSuffix);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Iterator<Integer> iterator() {
        return new Iterator<>() {
            /** */
            private int idx = 0;

            /** */
            private final int size = calculateSize();

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return idx < size;
            }

            /** {@inheritDoc} */
            @Override public Integer next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                Integer res = idx <= rangeEndInclusive ? idx : sparseSuffix.get(idx - rangeEndInclusive - 1);

                ++idx;

                return res;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rangeStartInclusive);
        out.writeInt(rangeEndInclusive);
        out.writeObject(sparseSuffix);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rangeStartInclusive = in.readInt();
        rangeEndInclusive = in.readInt();
        sparseSuffix = (GridIntList)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(rangeStartInclusive, rangeEndInclusive, sparseSuffix);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("IgniteFeatureSet [").append(rangeStartInclusive);

        if (rangeStartInclusive != rangeEndInclusive)
            sb.append(rangeEndInclusive - rangeStartInclusive > 1 ? " -> " : ", ").append(rangeEndInclusive);

        if (sparseSuffix != null) {
            for (GridIntIterator iter = sparseSuffix.iterator(); iter.hasNext(); )
                sb.append(", ").append(iter.next());
        }

        sb.append(']');

        return sb.toString();
    }

    /** */
    public static IgniteFeatureSet buildFrom(Class<?> cls) {
        return buildFrom(readDeclaredFeatures(cls));
    }

    /**
     * Creates an {@link IgniteFeatureSet} from the specified collection of {@link IgniteFeature}s that can be deactivated
     * on an Ignite node.
     *
     * <p>{@link IgniteFeature} instances that are not present in the specified collection and whose IDs are lower than
     * the minimum ID among the specified {@link IgniteFeature}s are considered non-deactivatable.</p>
     */
    public static IgniteFeatureSet buildFrom(Collection<? extends IgniteFeature> nodeFeatures) {
        A.notEmpty(nodeFeatures, "node features");

        GridIntList featureIds = new GridIntList(nodeFeatures.stream().mapToInt(IgniteFeature::id).toArray());

        featureIds.sort();

        int rangeStart = featureIds.get(0);
        int rangeEnd = rangeStart;

        int idx = 1;

        for (; idx < featureIds.size(); idx++) {
            int nextFeatureId = featureIds.get(idx);

            assert nextFeatureId != rangeEnd : "Duplication of Ignite Feature ID";

            if (nextFeatureId == rangeEnd + 1)
                rangeEnd = nextFeatureId;
            else
                break;
        }

        GridIntList sparseSuffix = idx < featureIds.size() ? featureIds.copyOfRange(idx, featureIds.size()) : null;

        return new IgniteFeatureSet(rangeStart, rangeEnd, sparseSuffix);
    }

    /** */
    static Collection<IgniteFeature> readDeclaredFeatures(Class<?> cls) {
        A.notNull(cls, "cls");

        List<IgniteFeature> features = new ArrayList<>();

        for (Field field : cls.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType().equals(IgniteFeature.class)) {
                if (field.getName().endsWith("_FEATURE")) {
                    try {
                        IgniteFeature feature = (IgniteFeature)field.get(null);

                        features.add(feature);
                    }
                    catch (IllegalAccessException e) {
                        throw new IgniteException(
                            "Failed to parse specified file to class to collect feature definitions [cls=" + cls.getName() + ']', e);
                    }
                }
            }
        }

        if (features.isEmpty())
            throw new IgniteException("No Ignite feature definitions were found in the specified class [cls=" + cls.getName() + ']');

        return features;
    }
}
