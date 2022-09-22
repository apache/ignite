package de.kp.works.ignite.gremlin.mutators;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.mutate.IgniteMutation;
import de.kp.works.ignite.mutate.IgnitePut;
import de.kp.works.ignite.query.IgniteResult;
import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.gremlin.exception.IgniteGraphException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Mutators {

    public static void create(IgniteTable table, Creator... creators) {

        List<IgniteMutation> batch = new ArrayList<>();
        for (Creator creator : creators) {
            Iterator<IgnitePut> insertions = creator.constructInsertions();
            insertions.forEachRemaining(batch::add);
        }
        write(table, batch);
    }

    private static void create(IgniteTable table, Creator creator, IgnitePut put) {

        boolean success = false;
        try {
            success = table.put(put);
        } catch (Exception e) {
            /* Do nothing */
        }

        if (!success) {
            throw creator.alreadyExists();
        }
    }

    public static void write(IgniteTable table, Mutator... writers) {
        List<IgniteMutation> batch = new ArrayList<>();
        for (Mutator writer : writers) {
            writer.constructMutations().forEachRemaining(batch::add);
        }
        write(table, batch);
    }

    public static long increment(IgniteTable table, Mutator writer, String key) {

        List<IgniteMutation> batch = new ArrayList<>();
        writer.constructMutations().forEachRemaining(batch::add);

        Object[] results = write(table, batch);

        // Increment result is the first
        IgniteResult result = (IgniteResult) results[0];
        Object value = result.getValue(key);

        if (value instanceof Exception) {
            throw new IgniteGraphException((Exception) value);
        }

        return (long)value;
    }

    private static Object[] write(IgniteTable table, List<IgniteMutation> mutations) {

        Object[] results = new Object[mutations.size()];
        if (mutations.isEmpty()) return results;

        try {
            table.batch(mutations, results);

            for (Object result : results) {
                if (result instanceof Exception) {
                    throw new IgniteGraphException((Exception) result);
                }
            }

        } catch (Exception e) {
            throw new IgniteGraphException(e);
        }

        return results;

    }
}
