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

package org.apache.ignite.internal.processors.bulkload.pipeline;

import org.apache.ignite.IgniteCheckedException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link PipelineBlock}, which splits input stream of char[] into lines using the specified {@link Pattern}
 * as line separator. Next block {@link PipelineBlock#accept(Object, boolean)} is invoked for each line.
 * Leftover characters are remembered and used during processing the next input batch,
 * unless isLastPortion flag is specified.
 */
public class LineSplitterBlock extends PipelineBlock<char[], String> {
    /** Line separator pattern */
    private final Pattern delim;

    /** Leftover characters from the previous invocation of {@link #accept(char[], boolean)}. */
    private StringBuilder leftover = new StringBuilder();

    /**
     * Creates line splitter block.
     *
     * @param delim The line separator pattern.
     */
    public LineSplitterBlock(Pattern delim) {
        this.delim = delim;
    }

    /** {@inheritDoc} */
    @Override public void accept(char[] chars, boolean isLastPortion) throws IgniteCheckedException {
        leftover.append(chars);

        String input = leftover.toString();
        Matcher matcher = delim.matcher(input);

        int lastPos = 0;
        while (matcher.find()) {
            String outStr = input.substring(lastPos, matcher.start());

            if (!outStr.isEmpty())
                nextBlock.accept(outStr, false);

            lastPos = matcher.end();
        }

        if (lastPos != 0)
            leftover.delete(0, lastPos);

        if (isLastPortion && leftover.length() > 0) {
            nextBlock.accept(leftover.toString(), true);
            leftover.setLength(0);
        }
    }
}
