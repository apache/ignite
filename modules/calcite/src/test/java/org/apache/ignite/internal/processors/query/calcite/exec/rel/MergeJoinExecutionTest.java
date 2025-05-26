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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.calcite.rel.core.JoinRelType.ANTI;
import static org.apache.calcite.rel.core.JoinRelType.FULL;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.calcite.rel.core.JoinRelType.SEMI;
import static org.hamcrest.core.IsEqual.equalTo;

/** */
@SuppressWarnings("TypeMayBeWeakened")
public class MergeJoinExecutionTest extends AbstractExecutionTest {
    /** */
    public static final Object[][] EMPTY = new Object[0][];

    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = 1;
        super.setup();
    }

    /** */
    @Test
    public void joinEmptyTables() {
        verifyJoin(EMPTY, EMPTY, INNER, EMPTY);
        verifyJoin(EMPTY, EMPTY, LEFT, EMPTY);
        verifyJoin(EMPTY, EMPTY, RIGHT, EMPTY);
        verifyJoin(EMPTY, EMPTY, FULL, EMPTY);
        verifyJoin(EMPTY, EMPTY, SEMI, EMPTY);
        verifyJoin(EMPTY, EMPTY, ANTI, EMPTY);
    }

    /** */
    @Test
    public void joinEmptyLeftTable() {
        Object[][] right = {
            {1, "Core"},
            {1, "OLD_Core"},
            {2, "SQL"}
        };

        verifyJoin(EMPTY, right, INNER, EMPTY);
        verifyJoin(EMPTY, right, LEFT, EMPTY);
        verifyJoin(EMPTY, right, RIGHT, new Object[][] {
            {null, null, "Core"},
            {null, null, "OLD_Core"},
            {null, null, "SQL"}
        });
        verifyJoin(EMPTY, right, FULL, new Object[][] {
            {null, null, "Core"},
            {null, null, "OLD_Core"},
            {null, null, "SQL"}
        });
        verifyJoin(EMPTY, right, SEMI, EMPTY);
        verifyJoin(EMPTY, right, ANTI, EMPTY);
    }

    /** */
    @Test
    public void joinEmptyRightTable() {
        Object[][] left = {
            {1, "Roman", null},
            {2, "Igor", 1},
            {3, "Alexey", 2}
        };

        verifyJoin(left, EMPTY, INNER, EMPTY);
        verifyJoin(left, EMPTY, LEFT, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", null},
            {3, "Alexey", null}
        });
        verifyJoin(left, EMPTY, RIGHT, EMPTY);
        verifyJoin(left, EMPTY, FULL, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", null},
            {3, "Alexey", null}
        });
        verifyJoin(left, EMPTY, SEMI, EMPTY);
        verifyJoin(left, EMPTY, ANTI, new Object[][] {
            {1, "Roman"},
            {2, "Igor"},
            {3, "Alexey"}
        });
    }

    /** */
    @Test
    public void testJoinBuffersInner() throws Exception {
        doTestJoinBuffers(INNER);
    }

    /** */
    @Test
    public void testJoinBuffersFull() throws Exception {
        doTestJoinBuffers(FULL);
    }

    /** */
    @Test
    public void testJoinBuffersLeft() throws Exception {
        doTestJoinBuffers(LEFT);
    }

    /** */
    @Test
    public void testJoinBuffersRight() throws Exception {
        doTestJoinBuffers(RIGHT);
    }

    /** */
    @Test
    public void testJoinBuffersAnti() throws Exception {
        doTestJoinBuffers(ANTI);
    }

    /** */
    @Test
    public void testJoinBuffersSemi() throws Exception {
        doTestJoinBuffers(SEMI);
    }

    /** */
    private void doTestJoinBuffers(JoinRelType joinType) throws Exception {
        int size = IN_BUFFER_SIZE * 2 + IN_BUFFER_SIZE / 2;
        int intersect = IN_BUFFER_SIZE / 10;

        int leftTo = size + intersect;
        int rightTo = size * 2;

        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        Iterator<Object[]> leftIter = IntStream.range(0, leftTo).boxed().map(i -> new Object[] {i}).iterator();
        Iterator<Object[]> rightIter = IntStream.range(size, rightTo).boxed().map(i -> new Object[] {i}).iterator();

        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class);
        ScanNode<Object[]> leftNode = new ScanNode<>(ctx, leftType, () -> leftIter);

        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class);
        ScanNode<Object[]> rightNode = new ScanNode<>(ctx, rightType, () -> rightIter);

        RelDataType outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, int.class);

        MergeJoinNode<Object[]> join = MergeJoinNode.create(ctx, outType, leftType, rightType, joinType,
            Comparator.comparingInt(r -> (Integer)r[0]), true);

        join.register(F.asList(leftNode, rightNode));

        List<Object[]> res = new ArrayList<>();
        AtomicBoolean finished = new AtomicBoolean();

        join.onRegister(new Downstream<>() {
            @Override public void push(Object[] objects) {
                res.add(objects);
            }

            @Override public void end() {
                finished.set(true);
            }

            @Override public void onError(Throwable e) {
                // No-op.
            }
        });

        join.request(size * size);

        assertTrue(GridTestUtils.waitForCondition(finished::get, getTestTimeout()));

        switch (joinType) {
            case LEFT:
                assertEquals(size + intersect, res.size());

                for (int i = 0; i < size; ++i) {
                    assertEquals(i, res.get(i)[0]);
                    assertEquals(null, res.get(i)[1]);
                }

                for (int i = size; i < size + intersect; ++i) {
                    assertEquals(i, res.get(i)[0]);
                    assertEquals(i, res.get(i)[1]);
                }
                break;
            case INNER:
                assertEquals(intersect, res.size());

                for (int i = size; i < size + intersect; ++i) {
                    assertEquals(i, res.get(i - size)[0]);
                    assertEquals(i, res.get(i - size)[1]);
                }
                break;
            case RIGHT:
                assertEquals(rightTo - size, res.size());

                for (int i = size; i < size + intersect; ++i) {
                    assertEquals(i, res.get(i - size)[0]);
                    assertEquals(i, res.get(i - size)[1]);
                }

                for (int i = size + intersect; i < size << 1; ++i) {
                    assertEquals(null, res.get(i - size)[0]);
                    assertEquals(i, res.get(i - size)[1]);
                }
                break;
            case FULL:
                assertEquals(size * 2, res.size());

                for (int i = 0; i < size; ++i) {
                    assertEquals(i, res.get(i)[0]);
                    assertEquals(null, res.get(i)[1]);
                }

                for (int i = size; i < size + intersect; ++i) {
                    assertEquals(i, res.get(i)[0]);
                    assertEquals(i, res.get(i)[1]);
                }

                for (int i = size + intersect; i < size << 1; ++i) {
                    assertEquals(null, res.get(i)[0]);
                    assertEquals(i, res.get(i)[1]);
                }
                break;
            case SEMI:
                assertEquals(intersect, res.size());

                for (int i = 0; i < intersect; ++i) {
                    assertEquals(1, res.get(i).length);
                    assertEquals(size + i, res.get(i)[0]);
                }
                break;
            case ANTI:
                assertEquals(size, res.size());

                for (int i = 0; i < size; ++i) {
                    assertEquals(1, res.get(i).length);
                    assertEquals(i, res.get(i)[0]);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + join);
        }

        assertTrue(join.leftInBuf.size() <= IN_BUFFER_SIZE);
        assertTrue(join.rightInBuf.size() <= IN_BUFFER_SIZE);
    }

    /** */
    @Test
    public void joinOneToMany() {
        Object[][] left = {
            {1, "Roman", null},
            {2, "Igor", 1},
            {3, "Alexey", 2}
        };

        Object[][] right = {
            {1, "Core"},
            {1, "OLD_Core"},
            {1, "NEW_Core"},
            {2, "SQL"},
            {3, "Arch"}
        };

        verifyJoin(left, right, INNER, new Object[][] {
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {2, "Igor", "NEW_Core"},
            {3, "Alexey", "SQL"}
        });
        verifyJoin(left, right, LEFT, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {2, "Igor", "NEW_Core"},
            {3, "Alexey", "SQL"}
        });
        verifyJoin(left, right, RIGHT, new Object[][] {
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {2, "Igor", "NEW_Core"},
            {3, "Alexey", "SQL"},
            {null, null, "Arch"}
        });
        verifyJoin(left, right, FULL, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {2, "Igor", "NEW_Core"},
            {3, "Alexey", "SQL"},
            {null, null, "Arch"}
        });
        verifyJoin(left, right, SEMI, new Object[][] {
            {2, "Igor"},
            {3, "Alexey"}
        });
        verifyJoin(left, right, ANTI, new Object[][] {
            {1, "Roman"}
        });
    }

    /** */
    @Test
    public void joinOneToMany2() {
        Object[][] left = {
            {1, "Roman", null},
            {2, "Igor", 1},
            {3, "Alexey", 2},
            {4, "Ivan", 4},
            {5, "Taras", 5},
            {6, "Lisa", 6}
        };

        Object[][] right = {
            {1, "Core"},
            {1, "OLD_Core"},
            {2, "SQL"},
            {3, "QA"},
            {5, "Arch"}
        };

        verifyJoin(left, right, INNER, new Object[][] {
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Alexey", "SQL"},
            {5, "Taras", "Arch"}
        });
        verifyJoin(left, right, LEFT, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Alexey", "SQL"},
            {4, "Ivan", null},
            {5, "Taras", "Arch"},
            {6, "Lisa", null}
        });
        verifyJoin(left, right, RIGHT, new Object[][] {
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Alexey", "SQL"},
            {null, null, "QA"},
            {5, "Taras", "Arch"}
        });
        verifyJoin(left, right, FULL, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Alexey", "SQL"},
            {null, null, "QA"},
            {4, "Ivan", null},
            {5, "Taras", "Arch"},
            {6, "Lisa", null}
        });
        verifyJoin(left, right, SEMI, new Object[][] {
            {2, "Igor"},
            {3, "Alexey"},
            {5, "Taras"}
        });
        verifyJoin(left, right, ANTI, new Object[][] {
            {1, "Roman"},
            {4, "Ivan"},
            {6, "Lisa"}
        });
    }

    /** */
    @Test
    public void joinManyToMany() {
        Object[][] left = {
            {1, "Roman", null},
            {2, "Igor", 1},
            {3, "Taras", 1},
            {4, "Alexey", 2},
            {5, "Ivan", 4},
            {6, "Andrey", 4}
        };

        Object[][] right = {
            {1, "Core"},
            {1, "OLD_Core"},
            {2, "SQL"},
            {3, "Arch"},
            {4, "QA"},
            {4, "OLD_QA"},
            {5, "Tx&PME"},
        };

        verifyJoin(left, right, INNER, new Object[][] {
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Taras", "Core"},
            {3, "Taras", "OLD_Core"},
            {4, "Alexey", "SQL"},
            {5, "Ivan", "QA"},
            {5, "Ivan", "OLD_QA"},
            {6, "Andrey", "QA"},
            {6, "Andrey", "OLD_QA"}
        });
        verifyJoin(left, right, LEFT, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Taras", "Core"},
            {3, "Taras", "OLD_Core"},
            {4, "Alexey", "SQL"},
            {5, "Ivan", "QA"},
            {5, "Ivan", "OLD_QA"},
            {6, "Andrey", "QA"},
            {6, "Andrey", "OLD_QA"}
        });
        verifyJoin(left, right, RIGHT, new Object[][] {
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Taras", "Core"},
            {3, "Taras", "OLD_Core"},
            {4, "Alexey", "SQL"},
            {null, null, "Arch"},
            {5, "Ivan", "QA"},
            {5, "Ivan", "OLD_QA"},
            {6, "Andrey", "QA"},
            {6, "Andrey", "OLD_QA"},
            {null, null, "Tx&PME"}
        });
        verifyJoin(left, right, FULL, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", "Core"},
            {2, "Igor", "OLD_Core"},
            {3, "Taras", "Core"},
            {3, "Taras", "OLD_Core"},
            {4, "Alexey", "SQL"},
            {null, null, "Arch"},
            {5, "Ivan", "QA"},
            {5, "Ivan", "OLD_QA"},
            {6, "Andrey", "QA"},
            {6, "Andrey", "OLD_QA"},
            {null, null, "Tx&PME"}
        });
        verifyJoin(left, right, SEMI, new Object[][] {
            {2, "Igor"},
            {3, "Taras"},
            {4, "Alexey"},
            {5, "Ivan"},
            {6, "Andrey"},
        });
        verifyJoin(left, right, ANTI, new Object[][] {
            {1, "Roman"}
        });
    }

    /** */
    @Test
    public void joinOnNullField() {
        Object[][] left = {
            {1, "Roman", null},
            {2, "Igor", null},
            {3, "Alexey", 1},
            {4, "Ivan", 2}
        };

        Object[][] right = {
            {null, "Core"},
            {null, "OLD_Core"},
            {1, "SQL"},
            {2, "QA"}
        };

        verifyJoin(left, right, INNER, new Object[][] {
            {3, "Alexey", "SQL"},
            {4, "Ivan", "QA"},
        });

        verifyJoin(left, right, LEFT, new Object[][] {
            {1, "Roman", null},
            {2, "Igor", null},
            {3, "Alexey", "SQL"},
            {4, "Ivan", "QA"},
        });

        verifyJoin(left, right, RIGHT, new Object[][] {
            {null, null, "Core"},
            {null, null, "OLD_Core"},
            {3, "Alexey", "SQL"},
            {4, "Ivan", "QA"},
        });

        verifyJoin(left, right, FULL, new Object[][] {
            {null, null, "Core"},
            {null, null, "OLD_Core"},
            {1, "Roman", null},
            {2, "Igor", null},
            {3, "Alexey", "SQL"},
            {4, "Ivan", "QA"},
        });

        verifyJoin(left, right, SEMI, new Object[][] {
            {3, "Alexey"},
            {4, "Ivan"},
        });

        verifyJoin(left, right, ANTI, new Object[][] {
            {1, "Roman"},
            {2, "Igor"},
        });
    }

    /**
     * Creates execution tree and executes it. Then compares the result of the execution with the given one.
     *
     * @param left Data for left table.
     * @param right Data for right table.
     * @param joinType Join type.
     * @param expRes Expected result.
     */
    private void verifyJoin(Object[][] left, Object[][] right, JoinRelType joinType, Object[][] expRes) {
        verifyJoin(left, right, joinType, expRes, false);
        verifyJoin(left, right, joinType, expRes, true);
    }

    /**
     * Creates execution tree and executes it. Then compares the result of the execution with the given one.
     *
     * @param left Data for left table.
     * @param right Data for right table.
     * @param joinType Join type.
     * @param expRes Expected result.
     * @param distr Distributed.
     */
    private void verifyJoin(Object[][] left, Object[][] right, JoinRelType joinType, Object[][] expRes, boolean distr) {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);

        RelDataType leftType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        ScanNode<Object[]> leftNode = new ScanNode<>(ctx, leftType, Arrays.asList(left));

        RelDataType rightType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);
        ScanNode<Object[]> rightNode = new ScanNode<>(ctx, rightType, Arrays.asList(right));

        RelDataType outType;
        if (setOf(SEMI, ANTI).contains(joinType))
            outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class);
        else
            outType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, Integer.class, int.class, String.class);

        MergeJoinNode<Object[]> join = MergeJoinNode.create(ctx, outType, leftType, rightType, joinType, (r1, r2) -> {
            Object o1 = r1[2];
            Object o2 = r2[0];

            if (o1 == null || o2 == null) {
                if (o1 != null)
                    return 1;
                else if (o2 != null)
                    return -1;
                else
                    return 1;
            }

            return Integer.compare((Integer)o1, (Integer)o2);
        }, distr);

        join.register(F.asList(leftNode, rightNode));

        RelDataType rowType;
        ProjectNode<Object[]> project;
        if (setOf(SEMI, ANTI).contains(joinType)) {
            rowType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class);
            project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[0], r[1]});
        }
        else {
            rowType = TypeUtils.createRowType(ctx.getTypeFactory(), int.class, String.class, String.class);
            project = new ProjectNode<>(ctx, rowType, r -> new Object[] {r[0], r[1], r[4]});
        }
        project.register(join);

        RootNode<Object[]> node = new RootNode<>(ctx, rowType);
        node.register(project);

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext())
            rows.add(node.next());

        Assert.assertThat(rows.toArray(EMPTY), equalTo(expRes));
    }

    /**
     * Creates {@link Set set} from provided items.
     *
     * @param items Items.
     * @return New set.
     */
    private static <T> Set<T> setOf(T... items) {
        return new HashSet<>(Arrays.asList(items));
    }
}
