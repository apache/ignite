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

const DFLT_IGFS = {
    defaultMode: {
        clsName: 'org.apache.ignite.igfs.IgfsMode',
        value: 'DUAL_ASYNC'
    },
    secondaryFileSystem: {

    },
    ipcEndpointConfiguration: {
        type: {
            clsName: 'org.apache.ignite.igfs.IgfsIpcEndpointType'
        },
        host: '127.0.0.1',
        port: 10500,
        memorySize: 262144,
        tokenDirectoryPath: 'ipc/shmem'
    },
    fragmentizerConcurrentFiles: 0,
    fragmentizerThrottlingBlockLength: 16777216,
    fragmentizerThrottlingDelay: 200,
    dualModeMaxPendingPutsSize: 0,
    dualModePutExecutorServiceShutdown: false,
    blockSize: 65536,
    streamBufferSize: 65536,
    maxSpaceSize: 0,
    maximumTaskRangeLength: 0,
    managementPort: 11400,
    perNodeBatchSize: 100,
    perNodeParallelBatchCount: 8,
    prefetchBlocks: 0,
    sequentialReadsBeforePrefetch: 0,
    trashPurgeTimeout: 1000,
    colocateMetadata: true,
    relaxedConsistency: true,
    pathModes: {
        keyClsName: 'java.lang.String',
        keyField: 'path',
        valClsName: 'org.apache.ignite.igfs.IgfsMode',
        valField: 'mode'
    }
};

export default class IgniteIGFSDefaults {
    constructor() {
        Object.assign(this, DFLT_IGFS);
    }
}
