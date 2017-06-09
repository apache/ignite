package org.apache.ignite.internal.processors.odbc.odbc;

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

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC query execute request with the batch of parameters.
 */
public class OdbcQueryExecuteBatchContinueRequest extends OdbcRequest {
    /** Query ID. */
    private final long queryId;

    /** Last param page flag. */
    private final boolean last;

    /** Sql query arguments. */
    @GridToStringExclude
    private final Object[][] args;

    /**
     * @param queryId Query ID.
     * @param last Last page flag.
     * @param args Arguments list.
     */
    public OdbcQueryExecuteBatchContinueRequest(long queryId, boolean last, Object[][] args) {
        super(QRY_EXEC_BATCH_CONTINUE);

        assert args != null : "Parameters should not be null";

        this.queryId = queryId;
        this.last = last;
        this.args = args;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * @return Sql query arguments.
     */
    public Object[][] arguments() {
        return args;
    }

    /**
     * @return Last page flag.
     */
    public boolean last() {
        return last;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryExecuteBatchContinueRequest.class, this, "args", args, true);
    }
}
