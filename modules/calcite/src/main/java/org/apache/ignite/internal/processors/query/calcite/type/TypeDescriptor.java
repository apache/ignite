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

package org.apache.ignite.internal.processors.query.calcite.type;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/**
 *
 */
public interface TypeDescriptor extends RelProtoDataType, DistributionAware, CollationAware {
    int cacheId();

    Map<String, Class<?>> fields();

    List<RelDataTypeField> extended();

    boolean matchType(CacheDataRow row);

    <T> T toRow(ExecutionContext ectx, GridCacheContext<?, ?> cctx, CacheDataRow row) throws IgniteCheckedException;

    TypeDescriptor extend(List<RelDataTypeField> fields);
}
