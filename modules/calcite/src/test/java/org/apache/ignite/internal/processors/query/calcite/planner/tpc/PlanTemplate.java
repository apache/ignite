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

package org.apache.ignite.internal.processors.query.calcite.planner.tpc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains logic to process plans templates.
 * Supported templates:
 * <ul>
 *     <li>{@code ", id = 123} replaced with the {@code ", id = {id}}</li>
 *     <li>{@code ", hash=123} replaced with the {@code ", hash={hash}}</li>
 *     <li>{@code "{INDEX_CASE:IDX1,IDX2}} expands to the plans where {INDEX_CASE....} replaced with each of the index from list.</li>
 *     <li>{@code "{ONEOF}} start and finish with the {@code {ONEOF}} tag. Cases separated with the line "=====".
 *     Creates as many plans as there are cases.
 *     </li>
 * </ul>
 */
public class PlanTemplate {
    /** */
    private static final Pattern ID_PATTERN = Pattern.compile(", id = \\d+");

    /** */
    private static final Pattern HASH_PATTERN = Pattern.compile(", hash=-?\\d+]");

    /** */
    public final String template;

    /** */
    public PlanTemplate(String template) {
        this.template = HASH_PATTERN.matcher(ID_PATTERN.matcher(template)
            .replaceAll(", id = {id}"))
            .replaceAll(", hash={hash}");
    }

    /**
     * @param actualPlan Plan to match.
     * @return {@code True} if any expanded plan equals to the parameter.
     */
    public boolean match(String actualPlan) {
        return  expandOneof(template).stream()
            .flatMap(PlanTemplate::expandIndexCase)
            .anyMatch(possiblePlan -> possiblePlan.equals(actualPlan));
    }

    /** Expands plans. Replace {INDEX_CASE} tag with the possible indexes. */
    private static Stream<String> expandIndexCase(String plan) {
        List<String> res = new ArrayList<>();

        res.add(plan);

        while (true) {
            String idxCaseStart = "{INDEX_CASE:";

            if (res.get(0).contains(idxCaseStart)) {
                res = res.stream().flatMap(p -> {
                    int start = p.indexOf(idxCaseStart);
                    int end = p.indexOf('}', start);

                    assert start != -1 && end != -1;

                    String[] idxs = p.substring(start + idxCaseStart.length(), end).split(",");

                    return Arrays.stream(idxs).map(idx -> p.substring(0, start) + idx + p.substring(end + 1));
                }).collect(Collectors.toList());
            }
            else
                break;
        }

        return res.stream();
    }

    /** Expands plans. Replace {ONEOF} tag with the possible cases. */
    private static List<String> expandOneof(String plan) {
        String oneOf = "{ONEOF}";

        if (!plan.contains(oneOf))
            return Collections.singletonList(plan);

        List<StringBuffer> res = new ArrayList<>();

        Scanner sc = new Scanner(plan);

        StringBuilder beforeOneof = new StringBuilder();

        while (sc.hasNextLine()) {
            String line = sc.nextLine();

            if (line.trim().startsWith(oneOf))
                break;

            beforeOneof.append(line).append('\n');
        }

        boolean oneOfEnd = false;

        // Expanding first found oneof
        while (sc.hasNextLine() && !oneOfEnd) {
            StringBuilder oneofCase = new StringBuilder();

            while (sc.hasNextLine()) {
                String line = sc.nextLine();

                oneOfEnd = line.trim().equals(oneOf);

                if (line.trim().matches("^=+$") || oneOfEnd)
                    break;

                oneofCase.append(line).append('\n');
            }

            res.add(new StringBuffer(beforeOneof).append(oneofCase));
        }

        if (!oneOfEnd)
            throw new IllegalStateException(oneOf + " not closed");

        while (sc.hasNextLine()) {
            String line = sc.nextLine();

            for (StringBuffer expanded : res) {
                expanded.append(line).append('\n');
            }
        }

        // Recursively expanding next ONEOF.
        return res.stream().flatMap(p -> expandOneof(p.toString()).stream()).collect(Collectors.toList());
    }
}
