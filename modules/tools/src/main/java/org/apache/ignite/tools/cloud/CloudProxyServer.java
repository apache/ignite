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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.tools.cloud.Handler.Cloud.AWS;
import static org.apache.ignite.tools.cloud.Handler.Cloud.GCI;
import static org.apache.ignite.tools.cloud.Handler.normalizeArgs;

/**
 *
 */
public class CloudProxyServer {
    /** Method map. */
    private static final Map<Handler.Cloud, List<Method>> methodMap = new HashMap<>();

    /** Sessions. */
    private static final Map<UUID, Session> sessions = new HashMap<>();

    /**
     * @param aCls A class.
     */
    private static List<Method> methods(Class aCls) {
        Method[] mthds = aCls.getMethods();

        List<Method> methods = new ArrayList<>();

        for (Method m : mthds)
            methods.add(m);

        return methods;
    }

    /**
     * @param args Args.
     */
    public static void main(String[] args) throws Exception {
        // Registrate aws client methods.
        methodMap.put(AWS, methods(AmazonS3.class));

        //Todo add other if needed
        try {
            String port = System.getProperty("port");

            ServerSocket ss = new ServerSocket(Integer.valueOf(port));

            System.out.println("Cloud proxy server started...");

            while (true) {
                Socket s = ss.accept();

                InputStream is = s.getInputStream();

                ObjectInputStream objIs = new ObjectInputStream(is);

                Handler.Request req = (Handler.Request)objIs.readObject();

                normalizeArgs(req.getArgs());

                System.out.println("Handle request:" + req);

                Session ses = sessions.get(req.getSesId());

                if (ses == null) {
                    System.out.println("Method " + req.getMtd() + " not found, args=" + Arrays.toString(req.getArgs())
                        + " sesId=" + req.getSesId());

                    sessions.put(req.getSesId(), ses = createSession(req));
                }

                Handler.Response res = ses.invoke(req);

                ObjectOutput objOut = new ObjectOutputStream(s.getOutputStream());

                System.out.println("Handle response:" + res);

                objOut.writeObject(res);

                checkAndCleanUp();

                objIs.close();
                objOut.close();
                s.close();
            }
        }
        catch (Throwable e) {
            e.printStackTrace();

            throw e;
        }
    }

    /**
     * @param req Request.
     */
    private static Session createSession(Handler.Request req) {
        if (req.getCloud() == AWS) {
            return new SessionImpl(
                new AmazonS3Client(new AWSCredentials() {
                    @Override public String getAWSAccessKeyId() {
                        return System.getProperty("aws.key.id");
                    }

                    @Override public String getAWSSecretKey() {
                        return System.getProperty("aws.key");
                    }
                }),
                methodMap.get(AWS)
            );
        }

        //todo Will be implemented in future.
        if (req.getCloud() == GCI)
            return NoopSession.noopSession;

        return NoopSession.noopSession;
    }

    /**
     *
     */
    private static void checkAndCleanUp() {
        long currTime = System.currentTimeMillis();

        for (Map.Entry<UUID, Session> entry : sessions.entrySet()) {
            Session ses = entry.getValue();

            if ((ses.lastInvoke() + ses.ttl()) > currTime)
                sessions.remove(entry.getKey());
        }
    }

    private interface Session {
        /**
         * @param req Request.
         */
        Handler.Response invoke(Handler.Request req);

        /**
         *
         */
        long lastInvoke();

        /**
         *
         */
        long ttl();
    }

    private static class NoopSession implements Session {
        /** Noop session. */
        private static final NoopSession noopSession = new NoopSession();

        /** Null response. */
        private static final Handler.Response nullResponse = new Handler.Response(null, null);

        /** {@inheritDoc} */
        @Override public Handler.Response invoke(Handler.Request req) {
            return nullResponse;
        }

        /** {@inheritDoc} */
        @Override public long lastInvoke() {
            return Long.MAX_VALUE;
        }

        /** {@inheritDoc} */
        @Override public long ttl() {
            return 0;
        }
    }

    /**
     *
     */
    private static class SessionImpl implements Session {
        /** Last invoke. */
        private long lastInvoke;

        /** Ttl. */
        private long ttl = 5 * 60 * 1000;

        /** Amazon s 3. */
        private Object client;

        /** Max invoke count. */
        private static final int maxInvokeCnt = 50;

        /** Invoke count. */
        private int invokeCnt;

        /** Method map. */
        private final List<Method> methods;

        /**
         *
         */
        private SessionImpl(Object client, List<Method> methods) {
            this.methods = methods;
            this.client = client;
        }

        /**
         * @param req Request.
         */
        @Override public Handler.Response invoke(Handler.Request req) {
            if (invokeCnt > maxInvokeCnt)
                return NoopSession.nullResponse;

            try {
                Object[] args = req.getArgs();

                Method m = findMethod(req);

                if (m == null)
                    return NoopSession.nullResponse;

                Object res = m.invoke(client, args);

                lastInvoke = System.currentTimeMillis();

                invokeCnt++;

                return new Handler.Response(res, null);
            }
            catch (Exception e) {
                e.printStackTrace();

                return new Handler.Response(null, e);
            }
        }

        /**
         * @param req Request.
         */
        private Method findMethod(Handler.Request req) {
            Object[] args = req.getArgs();

            Method m = null;

            method:
            for (Method method : methods)
                if (method.getName().equals(req.getMtd())) {

                    Class<?>[] types = method.getParameterTypes();

                    if (types.length != args.length)
                        continue;

                    for (int i = 0; i < types.length; i++)
                        if (!types[i].equals(args[i].getClass()) &&
                            !types[i].isAssignableFrom(args[i].getClass()))
                            continue method;

                    m = method;
                }

            return m;
        }


        /** {@inheritDoc} */
        @Override public long lastInvoke() {
            return lastInvoke;
        }

        /** {@inheritDoc} */
        @Override public long ttl() {
            return ttl;
        }
    }
}
