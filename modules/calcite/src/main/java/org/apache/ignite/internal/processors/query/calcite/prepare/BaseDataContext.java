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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.Locale;
import java.util.TimeZone;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Base data context.
 */
public final class BaseDataContext implements DataContext {
    /** */
    // TODO https://issues.apache.org/jira/browse/IGNITE-15276 Should be configurable.
    private final TimeZone timeZone = TimeZone.getDefault();

    /** */
    private static final SchemaPlus DFLT_SCHEMA = Frameworks.createRootSchema(false);

    /** */
    // TODO https://issues.apache.org/jira/browse/IGNITE-15276 Support other locales.
    private static final Locale LOCALE = Locale.ENGLISH;

    /**
     * Need to store timestamp, since SQL standard says that functions such as CURRENT_TIMESTAMP return the same value
     * throughout the query.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-15276 Should be propagated from the initiator node.
    private final long startTs;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    public BaseDataContext(IgniteTypeFactory typeFactory) {
        this.typeFactory = typeFactory;

        long ts = U.currentTimeMillis();
        startTs = ts + timeZone.getOffset(ts);
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getRootSchema() {
        return DFLT_SCHEMA;
    }

    /** {@inheritDoc} */
    @Override public IgniteTypeFactory getTypeFactory() {
        return typeFactory;
    }

    /** {@inheritDoc} */
    @Override public QueryProvider getQueryProvider() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        if (Variable.TIME_ZONE.camelName.equals(name))
            return timeZone;
        if (Variable.CURRENT_TIMESTAMP.camelName.equals(name))
            return startTs;
        if (Variable.LOCAL_TIMESTAMP.camelName.equals(name))
            return startTs;
        if (Variable.LOCALE.camelName.equals(name))
            return LOCALE;

        return null;
    }
}
