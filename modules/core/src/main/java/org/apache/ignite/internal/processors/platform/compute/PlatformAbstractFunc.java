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

package org.apache.ignite.internal.processors.platform.compute;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.PlatformSecurityAwareJob;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Base class for simple computations (Callable, Runnable).
 * Cleaner alternative to {@link PlatformClosureJob}, uses less wrapping for the underlying object,
 * and a single callback.
 */
public abstract class PlatformAbstractFunc implements PlatformSecurityAwareJob, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Serialized platform func. */
    private final Object func;

    /** Handle for local execution. */
    @SuppressWarnings("TransientFieldNotInitialized")
    private final transient long ptr;

    /** Ignite instance. */
    @IgniteInstanceResource
    protected transient Ignite ignite;

    /** Platform function name. */
    private final String funcName;

    /**
     * Constructor.
     *
     * @param func Platform func.
     * @param ptr Handle for local execution.
     * @param funcName Platform function name.
     */
    protected PlatformAbstractFunc(Object func, long ptr, String funcName) {
        this.ptr = ptr;
        assert func != null;

        this.func = func;
        this.funcName = funcName;
    }

    /**
     * Invokes this instance.
     *
     * @return Invocation result.
     */
    protected Object invoke() throws IgniteCheckedException {
        assert ignite != null;

        PlatformContext ctx = PlatformUtils.platformContext(ignite);

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            if (ptr != 0) {
                out.writeBoolean(true);
                out.writeLong(ptr);
            }
            else {
                out.writeBoolean(false);
                ctx.writer(out).writeObject(func);
            }

            out.synchronize();
            platformCallback(ctx.gateway(), mem.pointer());

            PlatformInputStream in = mem.input();
            in.synchronize();

            return PlatformUtils.readInvocationResult(ctx, ctx.reader(in));
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return funcName;
    }

    /**
     * Performs platform callback.
     *
     * @param gate Gateway.
     * @param memPtr Pointer.
     */
    protected abstract void platformCallback(PlatformCallbackGateway gate, long memPtr);
}
