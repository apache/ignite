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

 package org.apache.ignite.internal.processors.service;

 import java.util.concurrent.TimeUnit;
 import org.apache.ignite.Ignite;
 import org.apache.ignite.events.DiscoveryEvent;
 import org.apache.ignite.internal.IgniteEx;
 import org.apache.ignite.internal.events.DiscoveryCustomEvent;
 import org.apache.ignite.internal.managers.discovery.DiscoCache;
 import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
 import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
 import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
 import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
 import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
 import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
 import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
 import org.apache.ignite.internal.util.future.GridFutureAdapter;
 import org.apache.ignite.lang.IgniteInClosure;
 import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
 import org.junit.Test;
 import org.junit.runner.RunWith;
 import org.junit.runners.JUnit4;

 import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

 /**
  * <b>Tests in the class strongly depend on internal logic of discovery listeners and PME.</b>
  * <p/>
  * Tests that custom messages of custom discovery events which are being listened by {@link ServicesDeploymentManager}
  * will not be nullified before the manager will be able to capture them.
  * <p/>
  * Main purpose is to check the messages which may be nullified in PME process at the end of exchange in {@link
  * GridDhtPartitionsExchangeFuture#onDone()} and lock  {@link DiscoveryCustomEvent#customMessage(DiscoveryCustomMessage)}
  * which should prevent nullified before all discovery listeners will be able to handle them.
  *
  * @see #runTest(Class, IgniteInClosure)
  */
 @RunWith(JUnit4.class)
 public class ServiceDeploymentListenedCustomMessagesNotNullifiedTest extends GridCommonAbstractTest {
     /** */
     private static final long TEST_FUTURE_WAIT_TIMEOUT = 30_000L;

     /** */
     private static final long SLEEP_TIMEOUT = 2_000L;

     /** {@inheritDoc} */
     @Override protected void beforeTest() throws Exception {
         super.beforeTest();

         startGrid(0);
     }

     /** {@inheritDoc} */
     @Override protected void afterTest() throws Exception {
         super.afterTest();

         stopAllGrids();
     }

     /**
      * @throws Exception In case of an error.
      * @see #runTest(Class, IgniteInClosure)
      */
     @Test
     public void preventNullifyingDynamicCacheChangeBatchTest() throws Exception {
         runTest(DynamicCacheChangeBatch.class, new IgniteInClosure<Ignite>() {
             @Override public void apply(Ignite ignite) {
                 ignite.createCache("testCache");
             }
         });
     }

     /**
      * @throws Exception In case of an error.
      * @see #runTest(Class, IgniteInClosure)
      */
     @Test
     public void preventNullifyingChangeGlobalStateMessageTest() throws Exception {
         runTest(ChangeGlobalStateMessage.class, new IgniteInClosure<Ignite>() {
             @Override public void apply(Ignite ignite) {
                 assertTrue(ignite.cluster().active());

                 ignite.cluster().active(false);
             }
         });
     }

     /**
      * @throws Exception In case of an error.
      * @see #runTest(Class, IgniteInClosure)
      */
     @Test
     public void preventNullifyingCacheAffinityChangeMessageTest() throws Exception {
         runTest(CacheAffinityChangeMessage.class, new IgniteInClosure<Ignite>() {
             @Override public void apply(Ignite ignite) {
                 try {
                     startGrid(1);
                 }
                 catch (Exception e) {
                     fail("Failed to start instance, msg=" + e.getMessage());
                 }

                 ignite.createCache("testCache");

                 ignite.destroyCache("testCache");
             }
         });
     }

     /**
      * <b>Strongly depends on internal implementation of {@link GridEventStorageManager}.</b>
      * <p/>
      * Tests that custom message's (of given type) field of instance {@link DiscoveryCustomEvent} won't be nullified
      * before last discovery listener will be able to handel it.
      *
      * @param cls Class of expected messsage.
      * @param clo Test logic closure to generate a message of the expected type.
      * @throws Exception If failed.
      */
     protected void runTest(Class cls, IgniteInClosure<Ignite> clo) throws Exception {
         final IgniteEx ignite = grid(0);

         final GridFutureAdapter<Void> testResultFut = new GridFutureAdapter<>();

         // The registered listener will be the last added listener and will be notified last because all listeners are
         // stored using GridConcurrentLinkedHashSet in GridEventStorageManager. This guarantees that PME and Services
         // listeners have received the notification before the receiving event by the registered tests listener.
         ignite.context().event().addDiscoveryEventListener(new DiscoveryEventListener() {
             @SuppressWarnings("ErrorNotRethrown")
             @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
                 assertEquals(EVT_DISCOVERY_CUSTOM_EVT, evt.type());

                 try {
                     if (((DiscoveryCustomEvent)evt).customMessage().getClass().equals(cls)) {
                         try {
                             // Does not produce deadlock because PME's listener has been notified earlier.
                             awaitPartitionMapExchange();

                             doSleep(SLEEP_TIMEOUT);
                         }
                         catch (InterruptedException e) {
                             error("Failed to wait PME completion.", e);

                             testResultFut.onDone(e);
                         }

                         assertNotNull("Custom message has been nullified.", ((DiscoveryCustomEvent)evt).customMessage());

                         testResultFut.onDone();
                     }
                 }
                 catch (Error e) {
                     testResultFut.onDone(e);
                 }
             }
         }, EVT_DISCOVERY_CUSTOM_EVT);

         // Applies test logic to generate a message of the expected type.
         clo.apply(ignite);

         // Checks that custom message has not been nullified.
         testResultFut.get(TEST_FUTURE_WAIT_TIMEOUT, TimeUnit.MILLISECONDS);
     }
 }
