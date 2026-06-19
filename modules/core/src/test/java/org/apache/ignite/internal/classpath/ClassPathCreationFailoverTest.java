package org.apache.ignite.internal.classpath;

import java.nio.file.Path;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.classpath.ClassPathCreateCommand;
import org.apache.ignite.internal.management.classpath.ClassPathCreateCommandArg;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.classpath.ClassPathProcessor.metastorageKey;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class ClassPathCreationFailoverTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx failedNode;

    /** */
    private static boolean sendMessage;

    /** */
    private ListeningTestLogger lsnrLog;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        lsnrLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(lsnrLog)
            .setCommunicationSpi(new BlockingTcpCommunicationSpi());
    }

    // TODO: add stopped check during async operations.
    // TODO create classpath files on DownloadClassPathRequest - to check error while receiving file.

    /** */
    @Test
    public void testUploadNodeFailDuringDeployment() throws Exception {
        failedNode = startGrid(0);

        try {
            IgniteEx grid1 = startGrid(1);

            awaitPartitionMapExchange();

            Set<Path> cpFiles = ClassPathTestUtils.files();

            LogListener uploadNodeMsg = newClassPathListener();
            LogListener failMsg = logListener("Failed to download ClassPath files from remote node [icp=testcp]");
            LogListener rmvMsg =
                logListener("All nodes fail to deploy ClassPath. Will be removed. Retry creation [icp=testcp]");

            lsnrLog.registerAllListeners(uploadNodeMsg, failMsg, rmvMsg);

            ClassPathCreateCommandArg arg = new ClassPathCreateCommandArg();

            arg.name("testcp");
            arg.files(ClassPathTestUtils.fileArg(cpFiles));

            // ClassPath must be created on `failedNode`. Because, it first in cluster.
            assertThrows(
                null,
                () -> new ClassPathCreateCommand().execute(null, grid(0), arg, l -> failedNode.log().info(l)),
                ComputeTaskCancelledException.class,
                "Task cancelled due to stopping of the grid"
            );

            assertTrue(waitForCondition(uploadNodeMsg::check, 10_000));
            assertTrue(failedNode.context().isStopping());

            stopGrid(0);

            assertEquals(1, Ignition.allGrids().size());

            assertTrue(waitForCondition(() -> {
                ClassPathFilesTransmissionHandler icpFilesHnd
                    = getFieldValue(grid1.context().classPath(), "icpFilesHnd");

                Object t = getFieldValue(icpFilesHnd, "active");

                if (t == null)
                    return true;

                GridFutureAdapter<?> res = getFieldValue(t, "res");

                return res.isDone();
            }, 10_000));

            assertTrue(waitForCondition(failMsg::check, 10_000));
            assertTrue(waitForCondition(rmvMsg::check, 10_000));
            assertTrue(waitForCondition(
                () -> !grid1.context().pdsFolderResolver().fileTree().classPathRoot("testcp").exists(),
                10_000
            ));
            assertNull(grid1.context().distributedMetastorage().read(metastorageKey("testcp")));
        }
        finally {
            failedNode = null;
        }
    }

    /** */
    @Test
    public void testDownloadNodeFailDuringDeployment() throws Exception {
        startGrids(3);

        try {
            failedNode = grid(1);
            sendMessage = true;

            awaitPartitionMapExchange();

            Set<Path> cpFiles = ClassPathTestUtils.files();

            LogListener createdMsg = newClassPathListener();
            LogListener oneNodeFailMsg = logListener("Failed to download ClassPath files from remote node [icp=testcp]");
            LogListener readyMsg = logListener("ClassPath is READY. 2 of 3 nodes has its files");

            lsnrLog.registerAllListeners(createdMsg, oneNodeFailMsg, readyMsg);

            ClassPathCreateCommandArg arg = new ClassPathCreateCommandArg();

            arg.name("testcp");
            arg.files(ClassPathTestUtils.fileArg(cpFiles));

            // ClassPath must be created on `failedNode`. Because, it first in cluster.
            new ClassPathCreateCommand().execute(null, grid(0), arg, l -> failedNode.log().info(l));

            assertTrue(waitForCondition(createdMsg::check, 10_000));
            assertTrue(failedNode.context().isStopping());

            stopGrid(1);

            assertEquals(2, Ignition.allGrids().size());

            assertTrue(waitForCondition(oneNodeFailMsg::check, 10_000));
            assertTrue(waitForCondition(readyMsg::check, 10_000));

            ClassPathTestUtils.checkFilesExists(grid(0), "testcp", cpFiles);
            ClassPathTestUtils.checkFilesExists(grid(2), "testcp", cpFiles);
        }
        finally {
            failedNode = null;
            sendMessage = false;
        }
    }

    /** */
    protected static class BlockingTcpCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
            if (msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message() instanceof DownloadClassPathMessage
                && !failedNode.localNode().id().equals(ignite.cluster().localNode().id())
            ) {
                GridTestUtils.runAsync(() -> failedNode.close());

                if (!sendMessage)
                    return;
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    private LogListener newClassPathListener() {
        return logListener("New ClassPath created [uploadNode=" + grid(0).localNode().id());
    }

    /** */
    private static LogListener logListener(String msg) {
        return LogListener.matches(msg).times(1).build();
    }
}
