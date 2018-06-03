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

export default class Node {
    constructor(node) {
        this.nid = node.nodeId.toUpperCase();

        this.jvmPid = node.attributes['org.apache.ignite.jvm.pid'];
        this.macs = node.attributes['org.apache.ignite.macs'];

        this.ip = node.attributes['org.apache.ignite.ips'].split(',')[0];
        this.igniteVersion = node.attributes['org.apache.ignite.build.ver'];
        this.version = node.attributes['plugins.gg.build.ver'] || this.igniteVersion;
        // this.hostName = data.attributes[];
        this.clientMode = node.attributes['org.apache.ignite.cache.client'] ? 'CLIENT' : 'SERVER';
        this.gridName = node.attributes['org.apache.ignite.ignite.name'];

        this.startTime = node.metrics.startTime;
        this.upTime = node.metrics.upTime;

        this.cpus = node.metrics.totalCpus;

        this.heapMemoryMaximum = parseInt(node.metrics.heapMemoryMaximum, 10);
        this.heapMemoryUsed = parseInt(node.metrics.heapMemoryUsed, 10);
        this.heapMemoryCommitted = parseInt(node.metrics.heapMemoryCommitted, 10);

        this.busy = parseFloat(node.metrics.busyTimePercentage);

        this.cpuLoad = parseFloat(node.metrics.currentCpuLoad);
        this.gcLoad = parseFloat(node.metrics.currentGcCpuLoad);

        this.heapMemoryFreePercent = (this.heapMemoryMaximum - this.heapMemoryUsed) / this.heapMemoryMaximum;

        this.os = `${node.attributes['os.name']} ${node.attributes['os.arch']} ${node.attributes['os.version']}`;
    }

    static from(node) {
        return new Node(node);
    }
}
