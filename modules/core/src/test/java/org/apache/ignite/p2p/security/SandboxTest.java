package org.apache.ignite.p2p.security;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SandboxTest extends GridCommonAbstractTest {
    static final String CREATE_NEW_THREAD = "import org.apache.ignite.lang.IgniteRunnable;\n" +
        "public class SandboxRunnable implements IgniteRunnable {\n" +
        "        @Override public void run() {\n" +
        "           new Thread(() -> {throw new IllegalStateException(\"Thread shouldn't start\");}).start();\n" +
        "        }\n" +
        "    }";

    static final String READ_FROM_CACHE = "import org.apache.ignite.Ignition;\n" +
        "import org.apache.ignite.lang.IgniteCallable;\n" +
        "public class SandboxReadCallable implements IgniteCallable<Integer> {\n" +
        "    @Override public Integer call() throws Exception {\n" +
        "        return Ignition.localIgnite().<String, Integer>cache(\"test_cache\").get(\"key\");\n" +
        "    }\n" +
        "}";

    private Path srcTmpDir;

    @Before
    public void prepare() throws IOException {
        srcTmpDir = Files.createTempDirectory(getClass().getSimpleName());

        if (System.getSecurityManager() == null) {
            Policy.setPolicy(new Policy() {
                @Override
                public PermissionCollection getPermissions(CodeSource cs) {
                    Permissions res = new Permissions();
                    //по факту, это тоже самое что и запуск без SecurityManager, т.е.коду можно
                    // выполнять любые операции.
                    res.add(new AllPermission());

                    return res;
                }
            });

            System.setSecurityManager(new TestIgniteSecurityManager());
        }
    }

    @After
    public void cleanup() {
        U.delete(srcTmpDir);
    }

    /**
     * Негативный тест: должны завалиться при попытке создать новый поток, ибо нельзя.
     */
    @Test
    public void testShouldThrowExceptionWhenRunnableCreatesNewThread() throws Exception {
        URLClassLoader clsLdr = prepareClassLoader("SandboxRunnable.java", CREATE_NEW_THREAD);

        Class<?> cls = clsLdr.loadClass("SandboxRunnable");

        IgniteRunnable r = (IgniteRunnable)cls.newInstance();

        try {
            startGrids(2);

            IgniteEx init = grid(0);

            GridTestUtils.assertThrowsWithCause(
                () -> init.compute(init.cluster().forRemotes()).broadcast(r),
                AccessControlException.class
            );

        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Позититвный тест: должны получить значение кэша на удлаенном узле.
     */
    @Test
    public void testCallableShouldReturnValueFromCache() throws Exception {
        URLClassLoader clsLdr = prepareClassLoader("SandboxReadCallable.java", READ_FROM_CACHE);

        Class<?> cls = clsLdr.loadClass("SandboxReadCallable");

        IgniteCallable<Integer> callable = (IgniteCallable)cls.newInstance();

        try {
            startGrids(2);

            IgniteEx init = grid(0);

            Integer val = 101;

            init.createCache(
                new CacheConfiguration<String, Integer>().setName("test_cache")
            ).put("key", val);

            assertThat(init.compute(init.cluster().forRemotes()).call(callable), is(val));
        }
        finally {
            stopAllGrids();
        }
    }

    private URLClassLoader prepareClassLoader(String fileName, String src) throws Exception {
        Files.createDirectories(srcTmpDir); // To avoid possible NoSuchFileException on some OS.

        File srcFile = new File(srcTmpDir.toFile(), fileName);

        Path srcFilePath = Files.write(srcFile.toPath(), src.getBytes(StandardCharsets.UTF_8));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

        compiler.run(null, null, null, srcFilePath.toString());

        assertTrue("Failed to remove source file.", srcFile.delete());

        return new URLClassLoader(new URL[] {srcTmpDir.toUri().toURL()});
    }

    class FailRunnable implements IgniteRunnable {
        @Override public void run() {
            new Thread(() -> {throw new IllegalStateException("Thread shouldn't start");}).start();
        }
    }

}
