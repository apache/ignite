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

package org.apache.ignite.examples.portable.computegrid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.portable.PortableObject;
import org.jetbrains.annotations.Nullable;

/**
 * Task that is used for {@link ComputeClientPortableTaskExecutionExample} and
 * similar examples in .NET and C++.
 * <p>
 * This task calculates average salary for provided collection of employees.
 * It splits the collection into batches of size {@code 3} and creates a job
 * for each batch. After all jobs are executed, there results are reduced to
 * get the average salary.
 */
public class ComputeClientTask extends ComputeTaskSplitAdapter<Collection<PortableObject>, Long> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(
        int gridSize,
        Collection<PortableObject> arg
    ) {
        Collection<ComputeClientJob> jobs = new ArrayList<>();

        Collection<PortableObject> employees = new ArrayList<>();

        // Split provided collection into batches and
        // create a job for each batch.
        for (PortableObject employee : arg) {
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
        private final Collection<PortableObject> employees;

        /**
         * @param employees Collection of employees.
         */
        private ComputeClientJob(Collection<PortableObject> employees) {
            this.employees = employees;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            long sum = 0;
            int cnt = 0;

            for (PortableObject employee : employees) {
                System.out.println(">>> Processing employee: " + employee.field("name"));

                // Get salary from portable object. Note that object
                // doesn't need to be fully deserialized.
                long salary = employee.field("salary");

                sum += salary;
                cnt++;
            }

            return new IgniteBiTuple<>(sum, cnt);
        }
    }
}
