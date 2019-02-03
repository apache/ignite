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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown during serialization if class isn't registered and it's registration isn't allowed.
 */
public class UnregisteredClassException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Class cls;

    /**
     * @param cls Class that isn't registered.
     */
    public UnregisteredClassException(Class cls) {
        this.cls = cls;
    }

    /**
     * @param msg Error message.
     * @param cls Class that isn't registered.
     */
    public UnregisteredClassException(String msg, Class cls) {
        super(msg);
        this.cls = cls;
    }

    /**
     * @param cause Exception cause.
     * @param cls Class that isn't registered.
     */
    public UnregisteredClassException(Throwable cause, Class cls) {
        super(cause);
        this.cls = cls;
    }

    /**
     * @param msg Error message.
     * @param cause Exception cause.
     * @param cls Class that isn't registered.
     */
    public UnregisteredClassException(String msg, @Nullable Throwable cause, Class cls) {
        super(msg, cause);
        this.cls = cls;
    }

    /**
     * @return Class that isn't registered.
     */
    public Class cls() {
        return cls;
    }
}
