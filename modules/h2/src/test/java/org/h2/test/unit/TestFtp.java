/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.dev.ftp.FtpClient;
import org.h2.dev.ftp.server.FtpEvent;
import org.h2.dev.ftp.server.FtpEventListener;
import org.h2.dev.ftp.server.FtpServer;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the FTP server tool.
 */
public class TestFtp extends TestBase implements FtpEventListener {

    private FtpEvent lastEvent;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        if (getBaseDir().indexOf(':') > 0) {
            return;
        }
        FileUtils.delete(getBaseDir() + "/ftp");
        test(getBaseDir());
        FileUtils.delete(getBaseDir() + "/ftp");
    }

    private void test(String dir) throws Exception {
        Server server = FtpServer.createFtpServer(
                "-ftpDir", dir, "-ftpPort", "8121").start();
        FtpServer ftp = (FtpServer) server.getService();
        ftp.setEventListener(this);
        FtpClient client = FtpClient.open("localhost:8121");
        client.login("sa", "sa");
        client.makeDirectory("ftp");
        client.changeWorkingDirectory("ftp");
        assertEquals("CWD", lastEvent.getCommand());
        client.makeDirectory("hello");
        client.changeWorkingDirectory("hello");
        client.changeDirectoryUp();
        assertEquals("CDUP", lastEvent.getCommand());
        client.nameList("hello");
        client.removeDirectory("hello");
        client.close();
        server.stop();
    }

    @Override
    public void beforeCommand(FtpEvent event) {
        lastEvent = event;
    }

    @Override
    public void afterCommand(FtpEvent event) {
        lastEvent = event;
    }

    @Override
    public void onUnsupportedCommand(FtpEvent event) {
        lastEvent = event;
    }

}
