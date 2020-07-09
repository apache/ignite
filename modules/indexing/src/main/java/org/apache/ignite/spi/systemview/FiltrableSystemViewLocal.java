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

package org.apache.ignite.spi.systemview;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemViewColumnCondition;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeVisitor;
import org.h2.result.SearchRow;

/**
 * Filtrable SQL system view to export {@link SystemView} data.
 */
public class FiltrableSystemViewLocal<R> extends SystemViewLocal<R> {
    /** View attribute names. */
    private final String[] attributeNames;

    /** View attribute classes. */
    private final Class<?>[] attributeClasses;

    /**
     * @param ctx Kernal context.
     * @param sysView View to export.
     */
    public FiltrableSystemViewLocal(GridKernalContext ctx, SystemView<R> sysView) {
        super(ctx, sysView, indexes(sysView));

        assert sysView instanceof FiltrableSystemView;

        attributeNames = new String[sysView.walker().count()];
        attributeClasses = new Class<?>[sysView.walker().count()];

        sysView.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                attributeNames[idx] = name;
                attributeClasses[idx] = U.box(clazz);
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected Iterator<R> viewIterator(SearchRow first, SearchRow last) {
        Map<String, Object> filter = new HashMap<>();

        for (int i = 0; i < cols.length; i++) {
            SqlSystemViewColumnCondition cond = SqlSystemViewColumnCondition.forColumn(i, first, last);

            if (cond.isEquality()) {
                Object val = cond.valueForEquality().getObject();

                if (attributeClasses[i].isInstance(val))
                    filter.put(attributeNames[i], val);
                else {
                    throw new IgniteSQLException("Unexpected filter value type [column=" + cols[i] +
                        ", actual=" + val.getClass().getSimpleName() +
                        ", expected=" + attributeClasses[i].getSimpleName(),
                        IgniteQueryErrorCode.CONVERSION_FAILED);
                }
            }
        }

        return ((FiltrableSystemView<R>)sysView).iterator(filter);
    }

    /**
     * Extract indexes for specific {@link SystemView}.
     *
     * @param sysView System view.
     * @return Indexes array for {@code sysView}.
     */
    private static String[] indexes(SystemView<?> sysView) {
        return new String[] {sysView.walker().filtrableAttributes().stream().map(SystemViewLocal::sqlName)
                .collect(Collectors.joining(","))};
    }
}
