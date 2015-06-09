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

package org.apache.ignite.yarn;

import junit.framework.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.*;
import org.apache.hadoop.yarn.client.api.async.*;
import org.apache.hadoop.yarn.exceptions.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

/**
 * Application master tests.
 */
public class IgniteApplicationMasterSelfTest extends TestCase {
    /** */
    private ApplicationMaster appMaster;

    /** */
    private ClusterProperties props;

    /** */
    private RMMock rmMock = new RMMock();

    /**
     * @throws Exception If failed.
     */
    @Override protected void setUp() throws Exception {
        super.setUp();

        props = new ClusterProperties();
        appMaster = new ApplicationMaster("test", props);

        appMaster.setSchedulerTimeout(10000);

        rmMock.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testContainerAllocate() throws Exception {
        appMaster.setRmClient(rmMock);
        appMaster.setNmClient(new NMMock());

        props.cpusPerNode(2);
        props.memoryPerNode(1024);
        props.instances(3);

        Thread thread = runAppMaster(appMaster);

        List<AMRMClient.ContainerRequest> contRequests = collectRequests(rmMock, 2, 1000);

        interruptedThread(thread);

        assertEquals(3, contRequests.size());

        for (AMRMClient.ContainerRequest req : contRequests) {
            assertEquals(2, req.getCapability().getVirtualCores());
            assertEquals(1024, req.getCapability().getMemory());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterResource() throws Exception {
        rmMock.availableRes(new MockResource(1024, 2));

        appMaster.setRmClient(rmMock);
        appMaster.setNmClient(new NMMock());

        props.cpusPerNode(8);
        props.memoryPerNode(10240);
        props.instances(3);

        Thread thread = runAppMaster(appMaster);

        List<AMRMClient.ContainerRequest> contRequests = collectRequests(rmMock, 1, 1000);

        interruptedThread(thread);

        assertEquals(0, contRequests.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterAllocatedResource() throws Exception {
        rmMock.availableRes(new MockResource(1024, 2));

        appMaster.setRmClient(rmMock);
        appMaster.setNmClient(new NMMock());

        appMaster.setFs(new MockFileSystem());

        props.cpusPerNode(8);
        props.memoryPerNode(5000);
        props.instances(3);

        // Check that container resources
        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 5, 2000)));
        assertEquals(0, appMaster.getContainers().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 10, 2000)));
        assertEquals(0, appMaster.getContainers().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 1, 7000)));
        assertEquals(0, appMaster.getContainers().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 8, 5000)));
        assertEquals(1, appMaster.getContainers().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 10, 7000)));
        assertEquals(2, appMaster.getContainers().size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartReleaseContainer() throws Exception {
        rmMock.availableRes(new MockResource(1024, 2));

        NMMock nmClient = new NMMock();

        appMaster.setRmClient(rmMock);
        appMaster.setNmClient(nmClient);

        appMaster.setFs(new MockFileSystem());

        props.cpusPerNode(8);
        props.memoryPerNode(5000);
        props.instances(3);

        // Check that container resources
        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 5, 2000)));
        assertEquals(1, rmMock.releasedResources().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 5, 7000)));
        assertEquals(2, rmMock.releasedResources().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 9, 2000)));
        assertEquals(3, rmMock.releasedResources().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 8, 5000)));
        assertEquals(3, rmMock.releasedResources().size());
        assertEquals(1, nmClient.startedContainer().size());
    }


    /**
     * @throws Exception If failed.
     */
    public void testHostnameConstraint() throws Exception {
        rmMock.availableRes(new MockResource(1024, 2));

        NMMock nmClient = new NMMock();

        appMaster.setRmClient(rmMock);
        appMaster.setNmClient(nmClient);

        appMaster.setFs(new MockFileSystem());

        props.cpusPerNode(8);
        props.memoryPerNode(5000);
        props.instances(3);
        props.hostnameConstraint(Pattern.compile("ignoreHost"));

        // Check that container resources
        appMaster.onContainersAllocated(Collections.singletonList(createContainer("simple", 8, 5000)));
        assertEquals(0, rmMock.releasedResources().size());
        assertEquals(1, nmClient.startedContainer().size());

        appMaster.onContainersAllocated(Collections.singletonList(createContainer("ignoreHost", 8, 5000)));
        assertEquals(1, rmMock.releasedResources().size());
        assertEquals(1, nmClient.startedContainer().size());
    }

    /**
     * @param host Host.
     * @param cpu Cpu count.
     * @param mem Memory.
     * @return Container.
     */
    private Container createContainer(String host, int cpu, int mem) {
        return Container.newInstance(
            ContainerId.newContainerId(ApplicationAttemptId.newInstance(ApplicationId.newInstance(0l, 0), 0),
                ThreadLocalRandom.current().nextLong()),
            NodeId.newInstance(host, 0),
            "example.com",
            new MockResource(mem, cpu),
            Priority.newInstance(0),
            null
        );
    }

    /**
     * @param rmMock RM mock.
     * @param expectedCnt Expected cnt.
     * @param timeOut Timeout.
     * @return Requests.
     */
    private List<AMRMClient.ContainerRequest> collectRequests(RMMock rmMock, int expectedCnt, int timeOut) {
        long startTime = System.currentTimeMillis();

        List<AMRMClient.ContainerRequest> requests = rmMock.requests();

        while (requests.size() < expectedCnt
           && (System.currentTimeMillis() - startTime) < timeOut)
            requests = rmMock.requests();

        return requests;
    }

    /**
     * Runs appMaster other thread.
     *
     * @param appMaster Application master.
     * @return Thread.
     */
    private static Thread runAppMaster(final ApplicationMaster appMaster) {
        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    appMaster.run();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        thread.start();

        return thread;
    }

    /**
     * Interrupt thread and join.
     *
     * @param thread Thread.
     */
    private static void interruptedThread(Thread thread) throws InterruptedException {
        thread.interrupt();

        thread.join();
    }

    /**
     * Resource manager mock.
     */
    private static class RMMock extends AMRMClientAsync {
        /** */
        private List<AMRMClient.ContainerRequest> contRequests = new ArrayList<>();

        /** */
        private List<ContainerId> releasedConts = new ArrayList<>();

        /** */
        private Resource availableRes;

        /** */
        public RMMock() {
            super(0, null);
        }

        /**
         * @return Requests.
         */
        public List<AMRMClient.ContainerRequest> requests() {
            return contRequests;
        }

        /**
         * @return Released resources.
         */
        public List<ContainerId> releasedResources() {
            return releasedConts;
        }

        /**
         * Sets resource.
         *
         * @param availableRes Available resource.
         */
        public void availableRes(Resource availableRes) {
            this.availableRes = availableRes;
        }

        /**
         * Clear internal state.
         */
        public void clear() {
            contRequests.clear();
            releasedConts.clear();
            availableRes = null;
        }

        /** {@inheritDoc} */
        @Override public List<? extends Collection> getMatchingRequests(Priority priority, String resourceName,
            Resource capability) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public RegisterApplicationMasterResponse registerApplicationMaster(String appHostName,
            int appHostPort, String appTrackingUrl) throws YarnException, IOException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void unregisterApplicationMaster(FinalApplicationStatus appStatus, String appMessage,
            String appTrackingUrl) throws YarnException, IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void addContainerRequest(AMRMClient.ContainerRequest req) {
            contRequests.add(req);
        }

        /** {@inheritDoc} */
        @Override public void removeContainerRequest(AMRMClient.ContainerRequest req) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void releaseAssignedContainer(ContainerId containerId) {
            releasedConts.add(containerId);
        }

        /** {@inheritDoc} */
        @Override public Resource getAvailableResources() {
            return availableRes;
        }

        /** {@inheritDoc} */
        @Override public int getClusterNodeCount() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void updateBlacklist(List blacklistAdditions, List blacklistRemovals) {
            // No-op.
        }
    }

    /**
     * Network manager mock.
     */
    public static class NMMock extends NMClient {
        /** */
        private List<ContainerLaunchContext> startedContainer = new ArrayList<>();

        /** */
        public NMMock() {
            super("name");
        }

        /**
         * @return Started containers.
         */
        public List<ContainerLaunchContext> startedContainer() {
            return startedContainer;
        }

        /** {@inheritDoc} */
        @Override public Map<String, ByteBuffer> startContainer(Container container,
            ContainerLaunchContext containerLaunchContext) throws YarnException, IOException {

            startedContainer.add(containerLaunchContext);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void stopContainer(ContainerId containerId, NodeId nodeId) throws YarnException, IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ContainerStatus getContainerStatus(ContainerId containerId, NodeId nodeId)
            throws YarnException, IOException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void cleanupRunningContainersOnStop(boolean enabled) {
            // No-op.
        }
    }

    /**
     * Resource.
     */
    public static class MockResource extends Resource {
        /** Memory. */
        private int mem;

        /** CPU. */
        private int cpu;

        /**
         * @param mem Memory.
         * @param cpu CPU.
         */
        public MockResource(int mem, int cpu) {
            this.mem = mem;
            this.cpu = cpu;
        }

        /** {@inheritDoc} */
        @Override public int getMemory() {
            return mem;
        }

        /** {@inheritDoc} */
        @Override public void setMemory(int memory) {
            this.mem = memory;
        }

        /** {@inheritDoc} */
        @Override public int getVirtualCores() {
            return cpu;
        }

        /** {@inheritDoc} */
        @Override public void setVirtualCores(int vCores) {
            this.cpu = vCores;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(Resource resource) {
            return 0;
        }
    }

    /**
     * Mock file system.
     */
    public static class MockFileSystem extends FileSystem {
        /** */
        public MockFileSystem() {
        }

        /** {@inheritDoc} */
        @Override public Path makeQualified(Path path) {
            return new Path("/test/path");
        }

        /** {@inheritDoc} */
        @Override public FileStatus getFileStatus(Path f) throws IOException {
            return new FileStatus();
        }

        /** {@inheritDoc} */
        @Override public boolean mkdirs(Path f, FsPermission permission) throws IOException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Path getWorkingDirectory() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void setWorkingDirectory(Path new_dir) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
            return new FileStatus[0];
        }

        /** {@inheritDoc} */
        @Override public boolean delete(Path f, boolean recursive) throws IOException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean rename(Path src, Path dst) throws IOException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
            short replication, long blockSize, Progressable progress) throws IOException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FSDataInputStream open(Path f, int bufferSize) throws IOException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public URI getUri() {
            return null;
        }
    }
}
