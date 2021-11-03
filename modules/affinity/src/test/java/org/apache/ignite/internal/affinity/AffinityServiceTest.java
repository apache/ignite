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

package org.apache.ignite.internal.affinity;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * Tests scenarios for an affinity service. Please pay attention that given test doesn't check Rendezvous or any other affinity function it
 * just checks {@link AffinityUtils} logic.
 */
public class AffinityServiceTest {
    /**
     *
     */
    @Test
    public void testCalculatedAssignmentHappyPath() {
        List<List<ClusterNode>> assignments = AffinityUtils.calculateAssignments(
                Arrays.asList(
                        new ClusterNode(
                                UUID.randomUUID().toString(), "node0",
                                new NetworkAddress("localhost", 8080)
                        ),
                        new ClusterNode(
                                UUID.randomUUID().toString(), "node1",
                                new NetworkAddress("localhost", 8081)
                        )
                ),
                10,
                3
        );

        assertEquals(10, assignments.size());

        for (List<ClusterNode> partitionAssignment : assignments) {
            assertEquals(2, partitionAssignment.size());
        }
    }

    /**
     *
     */
    @Test
    public void testEmptyBaselineAssignmentsCalculation() {
        List<List<ClusterNode>> assignments = AffinityUtils.calculateAssignments(
                Collections.emptyList(),
                10,
                3
        );

        assertEquals(10, assignments.size());

        for (List<ClusterNode> partitionAssignment : assignments) {
            assertEquals(0, partitionAssignment.size());
        }
    }
}
