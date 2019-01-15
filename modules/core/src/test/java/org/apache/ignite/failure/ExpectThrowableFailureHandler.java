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

package org.apache.ignite.failure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Stops node and fails test.
 */
public class ExpectThrowableFailureHandler extends TestFailingFailureHandler {
    /** Expected throwables. */
    private final List<Class<? extends Throwable>> errs = new ArrayList<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param test Test.
     */
    public ExpectThrowableFailureHandler(GridAbstractTest test, IgniteLogger log) {
        super(test);
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
        for (Class<? extends Throwable> err : errs) {
            if (!X.hasCause(failureCtx.error(), err))
                continue;

            if (log.isInfoEnabled())
                log.info("Caught expected exception: " + failureCtx.error());

            return false;
        }

        return super.handle(ignite, failureCtx);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExpectThrowableFailureHandler.class, this);
    }

    /**
     * @param cls Expected throwable.
     * @return {@code true} (as specified by {@link Collection#add})
     */
    public boolean add(Class<? extends Throwable> cls) {
        return errs.add(cls);
    }

    /**
     * @param cls Expected throwable.
     * @return {@code true} (as specified by {@link Collection#remove})
     */
    public boolean remove(Class<? extends Throwable> cls) {
        return errs.remove(cls);
    }
}
