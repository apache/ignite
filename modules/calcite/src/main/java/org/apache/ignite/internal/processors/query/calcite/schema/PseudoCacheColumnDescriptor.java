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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.List;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.calcite.PseudoColumnDescriptor;
import org.apache.ignite.calcite.PseudoColumnProvider;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.PseudoColumnValueExtractorContextEx;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;

import static java.util.stream.Collectors.toList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;

/** Pseudocolumn descriptor for cache tables. */
class PseudoCacheColumnDescriptor implements CacheColumnDescriptor {
    /** */
    private static final ThreadLocal<PseudoColumnValueExtractorContextExImp> VALUE_EXTRACTOR_CTX = ThreadLocal.withInitial(
        PseudoColumnValueExtractorContextExImp::new
    );

    /** */
    private final PseudoColumnDescriptor desc;

    /** */
    private final int fieldIdx;

    /** */
    private volatile RelDataType logicalType;

    /** */
    PseudoCacheColumnDescriptor(PseudoColumnDescriptor desc, int fieldIdx) {
        this.desc = desc;
        this.fieldIdx = fieldIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean field() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean key() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Object value(
        ExecutionContext<?> ectx,
        GridCacheContext<?, ?> cctx,
        CacheDataRow src
    ) throws IgniteCheckedException {
        return desc.value(VALUE_EXTRACTOR_CTX.get().update(ectx, cctx, src));
    }

    /** {@inheritDoc} */
    @Override public void set(Object dst, Object val) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return desc.name();
    }

    /** {@inheritDoc} */
    @Override public int fieldIndex() {
        return fieldIdx;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory f) {
        if (logicalType == null) {
            logicalType = TypeUtils.sqlType(
                f,
                desc.type(),
                desc.precision() != PseudoColumnDescriptor.NOT_SPECIFIED ? desc.precision() : PRECISION_NOT_SPECIFIED,
                desc.scale() != PseudoColumnDescriptor.NOT_SPECIFIED ? desc.scale() : SCALE_NOT_SPECIFIED,
                true
            );
        }

        return logicalType;
    }

    /** {@inheritDoc} */
    @Override public Class<?> storageType() {
        return desc.type();
    }

    /** {@inheritDoc} */
    @Override public boolean hasDefaultValue() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Object defaultValue() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean pseudo() {
        return true;
    }

    /** */
    private static class PseudoColumnValueExtractorContextExImp implements PseudoColumnValueExtractorContextEx {
        /** */
        private GridCacheContext<?, ?> cctx;

        /** */
        private CacheDataRow src;

        /** */
        private ExecutionContext<?> ectx;

        /** */
        private PseudoColumnValueExtractorContextExImp update(
            ExecutionContext<?> ectx,
            GridCacheContext<?, ?> cctx,
            CacheDataRow src
        ) {
            this.ectx = ectx;
            this.cctx = cctx;
            this.src = src;

            return this;
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return cctx.cacheId();
        }

        /** {@inheritDoc} */
        @Override public String cacheName() {
            return cctx.name();
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return src.partition();
        }

        /** {@inheritDoc} */
        @Override public Object source(boolean keyOrValue, boolean keepBinary) {
            return cctx.unwrapBinaryIfNeeded(keyOrValue ? src.key() : src.value(), keepBinary, null);
        }

        /** {@inheritDoc} */
        @Override public ExecutionContext<?> executionCtx() {
            return ectx;
        }

        /** {@inheritDoc} */
        @Override public GridCacheContext<?, ?> cacheCtx() {
            return cctx;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow source() {
            return src;
        }
    }

    /** */
    static List<CacheColumnDescriptor> createCacheColDesc(int nxtColIdx, PseudoColumnProvider pseudoColProv) {
        int[] colIdx = {nxtColIdx};

        return pseudoColProv.provideDescriptors().stream()
            .map(d -> new PseudoCacheColumnDescriptor(d, colIdx[0]++))
            .collect(toList());
    }

    /**
     * Cheks that pseudocolumn names do not overlap with user-defined ones. This check is required for the persistent
     * cache node restart scenario. A suitable location for this check could not be found. For a running node, this
     * check was performed during the DDL validation phase.
     */
    static void checkForNameConflictsWithUserColumns(List<CacheColumnDescriptor> pseudoColDescs, GridQueryTypeDescriptor typeDesc) {
        Set<String> usrColNames = typeDesc.fields().keySet();

        for (CacheColumnDescriptor desc : pseudoColDescs) {
            if (usrColNames.contains(desc.name())) {
                throw new AssertionError(String.format(
                    "Pseudocolumn name should not overlap with the user ones: " +
                        "[pseudoColumnName=%s, cacheName=%s, schemaName=%s, tableName=%s]",
                    desc.name(), typeDesc.cacheName(), typeDesc.schemaName(), typeDesc.tableName()
                ));
            }
        }
    }
}
