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

package org.apache.ignite.loadtests.discovery;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class GridGcTimeoutTest {
    /** */
    public static final String CFG_PATH = "modules/core/src/test/config/discovery-stress.xml";

    /** */
    public static final int VALUE_SIZE = 1024;

    /**
     * @param args Args.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        Ignite g = G.start(U.resolveIgniteUrl(CFG_PATH));

        IgniteDataStreamer<Long, String> ldr = g.dataStreamer("default");

        ldr.perNodeBufferSize(16 * 1024);

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < VALUE_SIZE - 42; i++)
            sb.append('a');

        String str = sb.toString();
        long cntr = 0;

        while (true) {
            ldr.addData(cntr++, UUID.randomUUID() + str);

            if (cntr % 1000000 == 0)
                X.println("!!! Entries added: " + cntr);
        }
    }
}