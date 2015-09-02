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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.internal.portable.PortableRawWriterEx;

/**
 * Denotes an exception which has some data to be written in a special manner.
 */
public abstract class PlatformExtendedException extends PlatformException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Platform context. */
    protected final PlatformContext ctx;

    /**
     * Constructor.
     *
     * @param cause Root cause.
     * @param ctx Platform context.
     */
    protected PlatformExtendedException(Throwable cause, PlatformContext ctx) {
        super(cause);

        this.ctx = ctx;
    }

    /**
     * @return Platform context.
     */
    public PlatformContext context() {
        return ctx;
    }

    /**
     * Write data.
     *
     * @param writer Writer.
     */
    public abstract void writeData(PortableRawWriterEx writer);
}