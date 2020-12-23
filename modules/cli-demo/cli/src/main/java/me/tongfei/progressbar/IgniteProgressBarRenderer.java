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

package me.tongfei.progressbar;

import picocli.CommandLine.Help.Ansi;

/**
 * Custom renderer for the {@link ProgressBar}.
 *
 * Located in the {@code me.tongfei.progressbar} package because
 * the required {@link ProgressState} class is package-private.
 */
public class IgniteProgressBarRenderer implements ProgressBarRenderer {
    @Override public String render(ProgressState progress, int maxLength) {
        int completed = (int)(progress.getNormalizedProgress() * 100);

        StringBuilder sb = new StringBuilder("|");

        sb.append("=".repeat(completed));

        String percentage;
        int percentageLen;

        if (completed < 100) {
            sb.append('>').append(" ".repeat(99 - completed));

            percentage = completed + "%";
            percentageLen = percentage.length();
        }
        else {
            percentage = "@|green,bold Done!|@";
            percentageLen = 5;//percentageText.getCJKAdjustedLength();
        }

        sb.append("|").append(" ".repeat(6 - percentageLen)).append(percentage);

        return Ansi.AUTO.string(sb.toString());
    }
}
