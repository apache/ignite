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

package org.apache.ignite.testframework.junits;

import java.lang.reflect.InvocationTargetException;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteLogger;
import org.junit.internal.AssumptionViolatedException;

/** */
@FunctionalInterface
interface GridAbstractTestWithAssumption {
    /** */
    public void evaluateInvocation() throws Throwable;

    /** */
    public static void handleAssumption(GridAbstractTestWithAssumption src, IgniteLogger log) throws Throwable {
        BiConsumer<Throwable, IgniteLogger> report
            = (t, logger) -> logger.warning("Test skipped by assumption: " + t.getMessage(), t);

        try {
            src.evaluateInvocation();
        } catch (AssumptionViolatedException e) {
            report.accept(e, log);
        } catch (InvocationTargetException e) {
            Throwable actual = e.getTargetException();
            if (actual instanceof AssumptionViolatedException) {
                report.accept(e, log);

                return;
            }
            throw e;
        }
    }
}
