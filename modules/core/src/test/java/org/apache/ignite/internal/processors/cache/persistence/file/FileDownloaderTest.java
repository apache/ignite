package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import static org.junit.Assert.*;

public class FileDownloaderTest extends GridCommonAbstractTest {

    private static final Path DOWNLOADER_PATH = new File("download").toPath();
    private static final Path UPLOADER_PATH = new File("upload").toPath();

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (DOWNLOADER_PATH.toFile().exists())
            DOWNLOADER_PATH.toFile().delete();

        if (UPLOADER_PATH.toFile().exists())
            UPLOADER_PATH.toFile().delete();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (DOWNLOADER_PATH.toFile().exists())
            DOWNLOADER_PATH.toFile().delete();

        if (UPLOADER_PATH.toFile().exists())
            UPLOADER_PATH.toFile().delete();
    }

    public void test() throws Exception {
        //todo uncomment and fix
       /* assertTrue(UPLOADER_PATH.toFile().createNewFile());
        assertTrue(!DOWNLOADER_PATH.toFile().exists());

        PrintWriter writer = new PrintWriter(UPLOADER_PATH.toFile());

        for (int i = 0; i < 1_000_000; i++)
            writer.write("HELLO WORLD");

        writer.close();

        FileDownloader downloader = new FileDownloader(DOWNLOADER_PATH, Executors.newSingleThreadExecutor());

        InetSocketAddress address = downloader.start();

        FileUploader uploader = new FileUploader(UPLOADER_PATH, address, Executors.newSingleThreadExecutor());

        long size = uploader.upload().get();

        downloader.download(size).get();

        assertTrue(DOWNLOADER_PATH.toFile().exists());

        assertEquals(UPLOADER_PATH.toFile().length(), DOWNLOADER_PATH.toFile().length());

        assertArrayEquals(Files.readAllBytes(UPLOADER_PATH), Files.readAllBytes(DOWNLOADER_PATH));*/
    }
}