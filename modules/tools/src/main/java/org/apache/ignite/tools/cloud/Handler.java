/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tools.cloud;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Arrays;
import java.util.UUID;

/**
 * Local interceptor.
 */
public class Handler implements InvocationHandler {
    /** Cloud. */
    private final Cloud cloud;

    /** Session id. */
    private final UUID sesId;

    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /**
     * @param strCloud Cloud type.
     */
    public Handler(String strCloud) {
        sesId = UUID.randomUUID();
        cloud = Cloud.valueOf(strCloud);

        String hostAndPort = System.getProperty("server.cloud.proxy");

        host = hostAndPort.split(":")[0];
        port = Integer.valueOf(hostAndPort.split(":")[1]);
    }

    /** {@inheritDoc} */
    @Override public Object invoke(
        final Object proxy,
        final Method mtd,
        final Object[] args
    ) throws Throwable {
        normalizeArgs(args);

        return sendMessage(sesId, cloud, mtd, args);
    }

    /**
     * @param mtd Method.
     * @param args Args.
     */
    private Object sendMessage(UUID sesId, Cloud cloud, Method mtd, Object[] args) throws Throwable {
        Request msg = new Request(sesId, cloud, mtd.getName(), args);

        try (Socket sock = new Socket(host, port)) {

            InputStream in = sock.getInputStream();
            OutputStream out = sock.getOutputStream();

            ObjectOutputStream wr = new ObjectOutputStream(out);

            wr.writeObject(msg);
            wr.flush();

            ObjectInputStream objIns = new ObjectInputStream(in);

            Response o = (Response)objIns.readObject();

            objIns.close();
            wr.close();

            if (!o.hasException())
                return o.res;
            else
                throw o.ex;

        }
        catch (Exception e) {
            e.printStackTrace();

            throw new RuntimeException("Fail send request to proxy server " + host + ":" + port, e);
        }
    }

    /**
     * @param args Args.
     */
    static void normalizeArgs(Object[] args) throws IOException {
        for (int i = 0; i < args.length; i++){
            Object arg = args[i];

            if (arg instanceof byte[])
                args[i] = new ByteArrayInputStream((byte[])arg);

            if (arg instanceof ByteArrayInputStream){
                ByteArrayInputStream is = (ByteArrayInputStream)args[i];

                byte[] arr = new byte[is.available()];

                is.read(arr);

                args[i] = arr;
            }
        }
    }

    /**
     *
     */
    public enum Cloud {
        /** Aws. */
        AWS,

        /** Gci. */
        GCI
    }

    /**
     *
     */
    static class Request implements Serializable {
        /** Session id. */
        private UUID sesId;

        /** Cloud. */
        private Cloud cloud;

        /** Method. */
        private String mtd;

        /** Args. */
        private Object[] args;

        /**
         * @param sesId SesId.
         * @param mtd Method.
         * @param args Args.
         */
        private Request(
            UUID sesId,
            Cloud cloud,
            String mtd,
            Object[] args
        ) {
            this.sesId = sesId;
            this.cloud = cloud;
            this.mtd = mtd;
            this.args = args;
        }

        /**
         *
         */
        UUID getSesId() {
            return sesId;
        }

        /**
         *
         */
        Cloud getCloud() {
            return cloud;
        }

        /**
         *
         */
        String getMtd() {
            return mtd;
        }

        /**
         *
         */
        Object[] getArgs() {
            return args;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Request{" +
                "sesId=" + sesId +
                ", cloud=" + cloud +
                ", mtd='" + mtd + '\'' +
                ", args=" + Arrays.toString(args) +
                '}';
        }
    }

    /**
     *
     */
    static class Response implements Serializable {
        /** Result. */
        private final Object res;

        /** Exception. */
        private final Throwable ex;

        /**
         * @param res Result.
         * @param ex Exception.
         */
        Response(Object res, Throwable ex) {
            this.res = res;
            this.ex = ex;
        }

        /**
         *
         */
        boolean hasException() {
            return ex != null;
        }

        @Override public String toString() {
            return "Response{" +
                "res=" + res +
                ", ex=" + ex +
                '}';
        }
    }
}