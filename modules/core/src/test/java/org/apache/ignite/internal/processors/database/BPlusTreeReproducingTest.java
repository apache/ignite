package org.apache.ignite.internal.processors.database;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

//todo remove
public class BPlusTreeReproducingTest {
    /**
     * @throws Exception
     */
    @Test
    public void testSeveralInitShutdown() throws Exception {
        BPlusTreeFakeReuseSelfTest test = new BPlusTreeFakeReuseSelfTest() {
            @Override protected long getTestTimeout() {
                return 4 * 1000;
            }
        };

        Map<String, ThreadInfo> threadsBefore = getThreadsMap();

        //test.beforeTest();
        try {
             test.setName("testPutSizeLivelock");
             test.runBare();

            //test.setName("testTestRandomPutRemoveMultithreaded_3_70_1");
            //test.runBare();
        }
        catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        finally {
            //test.afterTest();
        }

        Map<String, ThreadInfo> threadInfoAfterMap = getThreadsMap();
        threadInfoAfterMap.remove("jvm-pause-detector-worker"); //daemon, exception

        if (threadsBefore.size() != threadInfoAfterMap.size()) {
            threadsBefore.keySet().forEach(threadInfoAfterMap::remove);

            if (!threadInfoAfterMap.isEmpty()) {

                for (Map.Entry<String, ThreadInfo> entry : threadInfoAfterMap.entrySet()) {
                    ThreadInfo info = entry.getValue();
                    String name = info.getThreadName();

                    System.out.println("Thread still running: " + name + " " + info);
                }

                assertTrue(threadInfoAfterMap.keySet().toString(), false);
            }
        }
    }

    private Map<String, ThreadInfo> getThreadsMap() {

        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        return threads(threadInfos);
    }

    private Map<String, ThreadInfo> threads(ThreadInfo[] threadInfos) {
        Map<String, ThreadInfo> map = new HashMap<>();

        for (ThreadInfo info : threadInfos) {
            map.put(info.getThreadName(), info);
        }

        return map;
    }
}
