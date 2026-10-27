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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.List;

/** Rows frame within window partition. */
abstract class WindowPartitionFrame<Row> {
    /** Holds immutable refrence to buffered window partition rows. */
    private final List<Row> buf;

    /** */
    WindowPartitionFrame(List<Row> buf) {
        this.buf = buf;
    }

    /** Returns row from partition by index. */
    Row get(int idx) {
        assert idx >= 0 && idx < buf.size() : "Invalid row index";
        return buf.get(idx);
    }

    /** Returns start frame index in partition for current row peer. */
    abstract int getFrameStart(int rowIdx, int peerIdx);

    /** Returns end frame index in partition for current row peer. */
    abstract int getFrameEnd(int rowIdx, int peerIdx);

    /** Returns frame size in partition for the current row peer. */
    final int size(int rowIdx, int peerIdx) {
        int start = getFrameStart(rowIdx, peerIdx);
        int end = getFrameEnd(rowIdx, peerIdx);
        if (end >= start)
            return end - start + 1;
        else
            return 0;
    }

    /** Returns row count in partition. */
    final int size() {
        return buf.size();
    }
}
