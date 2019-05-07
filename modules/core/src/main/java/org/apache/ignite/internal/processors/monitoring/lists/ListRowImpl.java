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

package org.apache.ignite.internal.processors.monitoring.lists;

/**
 *
 */
public class ListRowImpl<Id, Data> implements ListRow<Id, Data> {
    private Id id;

    private String sessionId;

    private Data data;

    public ListRowImpl(Id id, String sessionId, Data data) {
        this.id = id;
        this.sessionId = sessionId;
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public Id getId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String getSessionId() {
        return sessionId;
    }

    /** {@inheritDoc} */
    @Override public Data getData() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public void setData(Data data) {
        this.data = data;
    }
}
