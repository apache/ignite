/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.binary.computegrid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.binary.BinaryObject;
import org.jetbrains.annotations.Nullable;

/**
 * Task that is used for {@link ComputeClientBinaryTaskExecutionExample} and
 * similar examples in .NET and C++.
 * <p>
 * This task calculates average salary for provided collection of employees.
 * It splits the collection into batches of size {@code 3} and creates a job
 * for each batch. After all jobs are executed, there results are reduced to
 * get the average salary.
 */
public class ComputeClientTask extends ComputeTaskSplitAdapter<Collection<BinaryObject>, Long> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(
        int gridSize,
        Collection<BinaryObject> arg
    ) {
        Collection<ComputeClientJob> jobs = new ArrayList<>();

        Collection<BinaryObject> employees = new ArrayList<>();

        // Split provided collection into batches and
        // create a job for each batch.
        for (BinaryObject employee : arg) {
            employees.add(employee);

            if (employees.size() == 3) {
                jobs.add(new ComputeClientJob(employees));

                employees = new ArrayList<>(3);
            }
        }

        if (!employees.isEmpty())
            jobs.add(new ComputeClientJob(employees));

        return jobs;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Long reduce(List<ComputeJobResult> results) {
        long sum = 0;
        int cnt = 0;

        for (ComputeJobResult res : results) {
            IgniteBiTuple<Long, Integer> t = res.getData();

            sum += t.get1();
            cnt += t.get2();
        }

        return sum / cnt;
    }

    /**
     * Remote job for {@link ComputeClientTask}.
     */
    private static class ComputeClientJob extends ComputeJobAdapter {
        /** Collection of employees. */
        private final Collection<BinaryObject> employees;

        /**
         * @param employees Collection of employees.
         */
        private ComputeClientJob(Collection<BinaryObject> employees) {
            this.employees = employees;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            long sum = 0;
            int cnt = 0;

            for (BinaryObject employee : employees) {
                System.out.println(">>> Processing employee: " + employee.field("name"));

                // Get salary from binary object. Note that object
                // doesn't need to be fully deserialized.
                long salary = employee.field("salary");

                sum += salary;
                cnt++;
            }

            return new IgniteBiTuple<>(sum, cnt);
        }
    }
}
