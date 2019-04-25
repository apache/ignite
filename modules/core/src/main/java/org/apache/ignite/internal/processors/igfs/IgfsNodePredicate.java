/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS node predicate.
 */
public class IgfsNodePredicate implements IgnitePredicate<ClusterNode>, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS name. */
    private String igfsName;

    /**
     * Default constructor.
     */
    public IgfsNodePredicate() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param igfsName IGFS name.
     */
    public IgfsNodePredicate(@Nullable String igfsName) {
        this.igfsName = igfsName;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode node) {
        return IgfsUtils.isIgfsNode(node, igfsName);
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeString(igfsName);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader rawReader = reader.rawReader();

        igfsName = rawReader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsNodePredicate.class, this);
    }
}
