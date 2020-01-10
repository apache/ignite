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

package org.apache.ignite.internal.processors.query.calcite.message;


import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Message factory.
 */
public class CalciteMessageFactory implements MessageFactory {
    /** */
    static final short QUERY_START_REQUEST = 300;

    /** */
    static final short QUERY_CANCEL_REQUEST = 301;

    @Override public @Nullable Message create(short type) {
        switch (type) {
            case QUERY_START_REQUEST:
                return new QueryStartRequest();
            case QUERY_CANCEL_REQUEST:
                return new QueryCancelRequest();
        }

        return null;
    }


    // TODO all possible types


}
