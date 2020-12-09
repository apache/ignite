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

package org.apache.ignite.internal.processors.query.h2.database;

import org.h2.engine.Session;

/**
 * Holds H2 Session during work with index tree.
 */
public class ThreadLocalSessionHolder {
    /** */
    private static final ThreadLocal<Session> session = new ThreadLocal<>();

    /** */
    public static void setSession(Session ses) {
        session.set(ses);
    }

    /** */
    public static Session getSession() {
        return session.get();
    }

    /** */
    public static void removeSession() {
        session.remove();
    }
}
