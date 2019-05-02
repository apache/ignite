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

package org.apache.ignite.services;

/**
 *
 */
public class ServiceMonitoringInfo {
    /** Service name. */
    protected String name;

    /** Total count. */
    protected int totalCnt;

    /** Max per-node count. */
    protected int maxPerNodeCnt;

    /** Cache name. */
    protected String cacheName;

    public ServiceMonitoringInfo(String name, int totalCnt, int maxPerNodeCnt, String cacheName) {
        this.name = name;
        this.totalCnt = totalCnt;
        this.maxPerNodeCnt = maxPerNodeCnt;
        this.cacheName = cacheName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTotalCnt() {
        return totalCnt;
    }

    public void setTotalCnt(int totalCnt) {
        this.totalCnt = totalCnt;
    }

    public int getMaxPerNodeCnt() {
        return maxPerNodeCnt;
    }

    public void setMaxPerNodeCnt(int maxPerNodeCnt) {
        this.maxPerNodeCnt = maxPerNodeCnt;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }
}
