/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain;

public class GridNativeProcessor {

    static {
        System.loadLibrary("gridgain_client_bridge");
    }

    public static void myMethod(long addr, int len, long fut) {
        System.out.println("Invoked method [addr=" + addr + ", len=" + len + ", fut=" + fut + ']');

        notifyFuture0(fut);
    }

    public static native void notifyFuture0(long fut);
}
