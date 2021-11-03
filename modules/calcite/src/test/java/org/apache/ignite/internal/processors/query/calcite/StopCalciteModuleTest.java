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

package org.apache.ignite.internal.processors.query.calcite;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.event.TableEvent;
import org.apache.ignite.internal.table.event.TableEventParameters;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.TopologyService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Stop Calcite module test.
 */
@ExtendWith(MockitoExtension.class)
public class StopCalciteModuleTest {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(StopCalciteModuleTest.class);
    
    private static final int ROWS = 5;
    
    private static final String NODE_NAME = "mock-node-name";
    
    @Mock
    ClusterService clusterSrvc;
    
    @Mock
    TableManager tableManager;
    
    @Mock
    MessagingService msgSrvc;
    
    @Mock
    TopologyService topologySrvc;
    
    @Mock
    ClusterLocalConfiguration localCfg;
    
    @Mock
    InternalTable tbl;
    
    SchemaRegistry schemaReg;
    
    @BeforeEach
    public void before(TestInfo testInfo) {
        when(clusterSrvc.messagingService()).thenReturn(msgSrvc);
        when(clusterSrvc.topologyService()).thenReturn(topologySrvc);
        when(clusterSrvc.localConfiguration()).thenReturn(localCfg);
        
        ClusterNode node = new ClusterNode("mock-node-id", NODE_NAME, null);
        when(topologySrvc.localMember()).thenReturn(node);
        when(topologySrvc.allMembers()).thenReturn(Collections.singleton(node));
        
        SchemaDescriptor schemaDesc = new SchemaDescriptor(
                0,
                new Column[]{new Column("ID", NativeTypes.INT32, false)},
                new Column[]{new Column("VAL", NativeTypes.INT32, false)}
        );
        
        schemaReg = new SchemaRegistryImpl(0, (v) -> schemaDesc);
        
        when(tbl.tableName()).thenReturn("PUBLIC.TEST");
        
        // Mock create table (notify on register listener).
        doAnswer(invocation -> {
            EventListener<TableEventParameters> clo = (EventListener<TableEventParameters>) invocation.getArguments()[1];
            
            clo.notify(new TableEventParameters(new IgniteUuid(UUID.randomUUID(), 0), "TEST", new TableImpl(tbl, schemaReg, tableManager)),
                    null);
            
            return null;
        }).when(tableManager).listen(eq(TableEvent.CREATE), any());
        
        RowAssembler asm = new RowAssembler(schemaReg.schema(), 0, 0, 0, 0);
        
        asm.appendInt(0);
        asm.appendInt(0);
        
        BinaryRow binaryRow = asm.build();
        
        // Mock table scan
        doAnswer(invocation -> {
            int part = (int) invocation.getArguments()[0];
            
            return (Flow.Publisher<BinaryRow>) s -> {
                s.onSubscribe(new Flow.Subscription() {
                    @Override
                    public void request(long n) {
                        // No-op.
                    }
                    
                    @Override
                    public void cancel() {
                        // No-op.
                    }
                });
                
                if (part == 0) {
                    for (int i = 0; i < ROWS; ++i) {
                        s.onNext(binaryRow);
                    }
                }
                
                s.onComplete();
            };
        }).when(tbl).scan(anyInt(), any());
        
        LOG.info(">>>> Starting test {}", testInfo.getTestMethod().orElseThrow().getName());
    }
    
    /**
     *
     */
    @Test
    public void testStopQueryOnNodeStop() throws Exception {
        SqlQueryProcessor qryProc = new SqlQueryProcessor(clusterSrvc, tableManager);
        
        qryProc.start();
        
        List<SqlCursor<List<?>>> cursors = qryProc.query(
                "PUBLIC",
                "SELECT * FROM TEST"
        );
        
        SqlCursor<List<?>> cur = cursors.get(0);
        cur.next();
        
        assertTrue(isThereNodeThreads(NODE_NAME));
        
        qryProc.stop();
        
        // Check cursor closed.
        assertTrue(assertThrows(IgniteException.class, cur::hasNext).getMessage().contains("Query was cancelled"));
        assertTrue(assertThrows(IgniteException.class, cur::next).getMessage().contains("Query was cancelled"));
        
        // Check execute query on stopped node.
        assertTrue(assertThrows(IgniteException.class, () -> qryProc.query(
                "PUBLIC",
                "SELECT 1"
        )).getCause() instanceof NodeStoppingException);
        
        // Check: there are no alive Ignite threads.
        assertFalse(isThereNodeThreads(NODE_NAME));
    }
    
    /**
     * @return {@code true} is there are any threads with node name prefix; Otherwise returns {@code false}.
     */
    private boolean isThereNodeThreads(String nodeName) {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);
        
        return Arrays.stream(infos)
                .anyMatch((ti) -> ti.getThreadName().contains(nodeName));
    }
}


