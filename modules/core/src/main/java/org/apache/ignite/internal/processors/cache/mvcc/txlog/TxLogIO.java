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

package org.apache.ignite.internal.processors.cache.mvcc.txlog;

/**
 *
 */
public interface TxLogIO {
    /**
     * @param pageAddr Page address.
     * @param off Item offset.
     * @param row Row to compare with.
     * @return Comparision result.
     */
    int compare(long pageAddr, int off, TxKey row);

    /**
     * @param pageAddr Page address.
     * @param off Item offset.
     * @return Major version
     */
    long getMajor(long pageAddr, int off);

    /**
     * @param pageAddr Page address.
     * @param off Item offset.
     * @param major Major version
     */
    void setMajor(long pageAddr, int off, long major);

    /**
     * @param pageAddr Page address.
     * @param off Item offset.
     * @return Minor version.
     */
    long getMinor(long pageAddr, int off);

    /**
     * @param pageAddr Page address.
     * @param off Item offset.
     * @param minor Minor version.
     */
    void setMinor(long pageAddr, int off, long minor);

    /**
     * @param pageAddr Page address.
     * @param off Item offset.
     * @return Transaction state.
     */
    byte getState(long pageAddr, int off);

    /**
     * @param pageAddr Page address.
     * @param off Item offset.
     * @param state Transaction state.
     */
    void setState(long pageAddr, int off, byte state);
}
