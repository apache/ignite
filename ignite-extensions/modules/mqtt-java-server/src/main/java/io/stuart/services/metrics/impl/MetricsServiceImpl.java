/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.services.metrics.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.apache.ignite.IgniteAtomicLong;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.config.Config;
import io.stuart.consts.MetricsConst;
import io.stuart.consts.ParamConst;
import io.stuart.entities.metrics.MetricsQuadTuple;
import io.stuart.entities.metrics.MqttMetrics;
import io.stuart.entities.metrics.NodeMetrics;
import io.stuart.log.Logger;
import io.stuart.services.cache.CacheService;
import io.stuart.services.cache.impl.ClsCacheServiceImpl;
import io.stuart.services.cache.impl.StdCacheServiceImpl;
import io.stuart.services.metrics.MetricsService;
import io.stuart.utils.IdUtil;
import io.stuart.utils.MetricsUtil;

public class MetricsServiceImpl implements MetricsService {

    private static volatile MetricsService instance;

    private CacheService cacheService;

    private final ExecutorService executorService;

    private MetricsServiceImpl() {
        // get cluster mode
        String clusterMode = Config.getClusterMode();

        if (ParamConst.CLUSTER_MODE_STD.equalsIgnoreCase(clusterMode)) {
            // get standalone cache service
            this.cacheService = StdCacheServiceImpl.getInstance();
        } else {
            // get clustered cache service
            this.cacheService = ClsCacheServiceImpl.getInstance();
        }

        // initialize executor service
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public static MetricsService getInstance() {
        if (instance == null) {
            synchronized (MetricsServiceImpl.class) {
                if (instance == null) {
                    instance = new MetricsServiceImpl();
                }
            }
        }

        return instance;
    }

    @Override
    public void start() {
        // get retain cache size
        int size = cacheService.countRetains();
        // get retain count
        IgniteAtomicLong retainCount = cacheService.getIgnite().atomicLong(MetricsConst.SM_RETAIN_COUNT, 0, true);
        // get retain max
        IgniteAtomicLong retainMax = cacheService.getIgnite().atomicLong(MetricsConst.SM_RETAIN_MAX, 0, true);

        if (retainCount != null && !retainCount.removed()) {
            // set new count value
            retainCount.getAndSet(size);
        }
        if (retainMax != null && !retainMax.removed() && size > retainMax.get()) {
            // set new max value
            retainMax.getAndSet(size);
        }

        // initialize node statistics metrics
        NodeMetrics.getInstance();
        // initialize mqtt statistics metrics
        MqttMetrics.getInstance();
    }

    @Override
    public void stop() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public boolean isEnabled() {
        return Config.isMqttMetricsEnable();
    }

    @Override
    public void grecord(String clientId, long point, long... values) {
        if (!isEnabled() || point < 600 || point > 699 || values == null) {
            return;
        }
        if ((point == MetricsConst.GPN_MESSAGE_RECEIVED || point == MetricsConst.GPN_MESSAGE_SENT) && values.length != 2) {
            return;
        }
        if ((point != MetricsConst.GPN_MESSAGE_RECEIVED && point != MetricsConst.GPN_MESSAGE_SENT) && values.length != 1) {
            return;
        }

        // tuples
        List<MetricsQuadTuple> tuples = new ArrayList<>();
        // tuple
        MetricsQuadTuple tuple = null;

        // grouped point
        long gpoint = point;
        // qos
        long gqos = -1;

        if (point == MetricsConst.GPN_MESSAGE_RECEIVED) {
            // get qos
            gqos = values[0];

            if (gqos == MqttQoS.AT_MOST_ONCE.value()) {
                gpoint = MetricsConst.GPN_QOS0_RECEIVED;
            } else if (gqos == MqttQoS.AT_LEAST_ONCE.value()) {
                gpoint = MetricsConst.GPN_QOS1_RECEIVED;
            } else if (gqos == MqttQoS.EXACTLY_ONCE.value()) {
                gpoint = MetricsConst.GPN_QOS2_RECEIVED;
            }
        } else if (point == MetricsConst.GPN_MESSAGE_SENT) {
            // get qos
            gqos = values[0];

            if (gqos == MqttQoS.AT_MOST_ONCE.value()) {
                gpoint = MetricsConst.GPN_QOS0_SENT;
            } else if (gqos == MqttQoS.AT_LEAST_ONCE.value()) {
                gpoint = MetricsConst.GPN_QOS1_SENT;
            } else if (gqos == MqttQoS.EXACTLY_ONCE.value()) {
                gpoint = MetricsConst.GPN_QOS2_SENT;
            }
        }

        // get metrics quotas
        List<Long> quotas = MetricsUtil.getGroupedQuotas().get(gpoint);
        // quota
        long quota = -1;

        if (point == MetricsConst.GPN_MESSAGE_RECEIVED || point == MetricsConst.GPN_MESSAGE_SENT) {
            for (int i = 0, len = quotas.size(); i < len; ++i) {
                // get quota
                quota = quotas.get(i);

                if (MetricsConst.PN_SM_BYTE_RECEIVED != quota && MetricsConst.PN_SM_BYTE_SENT != quota) {
                    // get metrics quad tuple
                    tuple = tuple(clientId, quota, 1);
                } else {
                    // get metrics quad tuple
                    tuple = tuple(clientId, quota, values[1]);
                }

                if (tuple != null) {
                    tuples.add(tuple);
                }
            }
        } else {
            for (int i = 0, len = quotas.size(); i < len; ++i) {
                // get quota
                quota = quotas.get(i);
                // get metrics quad tuple
                tuple = tuple(clientId, quota, values[0]);

                if (tuple != null) {
                    tuples.add(tuple);
                }
            }
        }

        // update tuples
        update(tuples);
    }

    @Override
    public void grecord(long point, long... values) {
        grecord(null, point, values);
    }

    @Override
    public void record(String clientId, long... pairs) {
        if (!isEnabled() || pairs == null || pairs.length % 2 != 0) {
            return;
        }

        // tuples
        List<MetricsQuadTuple> tuples = new ArrayList<>();
        // tuple
        MetricsQuadTuple tuple = null;

        for (int i = 0, len = pairs.length; i < len; i = i + 2) {
            // get metrics quad tuple
            tuple = tuple(clientId, pairs[i], pairs[i + 1]);

            if (tuple != null) {
                tuples.add(tuple);
            }
        }

        // update tuples
        update(tuples);
    }

    @Override
    public void record(long... pairs) {
        record(null, pairs);
    }

    @Override
    public long getRetainCount() {
        // get retain count
        IgniteAtomicLong count = cacheService.getIgnite().atomicLong(MetricsConst.SM_RETAIN_COUNT, 0, false);

        if (count != null && !count.removed()) {
            return count.get();
        }

        return 0;
    }

    @Override
    public long getRetainMax() {
        // get retain max
        IgniteAtomicLong max = cacheService.getIgnite().atomicLong(MetricsConst.SM_RETAIN_MAX, 0, false);

        if (max != null && !max.removed()) {
            return max.get();
        }

        return 0;
    }

    private MetricsQuadTuple tuple(String clientId, long point, long value) {
        if (point < 100) {
            return sessionTuple(clientId, point, value);
        } else if (point < 200) {
            return nodeTuple(point, value);
        } else if (point < 300) {
            return packetTuple(point, value);
        } else if (point < 400) {
            return messageTuple(point, value);
        } else if (point < 500) {
            return byteTuple(point, value);
        } else if (point < 600) {
            return retainTuple(point, value);
        }

        return null;
    }

    private void update(List<MetricsQuadTuple> tuples) {
        if (tuples == null || tuples.isEmpty()) {
            return;
        }

        // submit metrics task
        submit(() -> {
            // metrics quota
            Object quota = null;
            // metrics function
            Function<MetricsQuadTuple, Boolean> func = null;

            for (MetricsQuadTuple tuple : tuples) {
                if (tuple == null) {
                    continue;
                }

                // get metrics quota
                quota = tuple.getQuota();
                // get metrics function
                func = tuple.getFunc();

                if (quota == null) {
                    continue;
                }

                if (func != null && !func.apply(tuple)) {
                    continue;
                }

                try {
                    if (quota instanceof String) {
                        // get quota atomic long
                        IgniteAtomicLong target = cacheService.getIgnite().atomicLong((String) quota, 0, false);

                        if (target == null || target.removed()) {
                            continue;
                        }

                        if (tuple.isCresc()) {
                            target.getAndAdd(tuple.getValue());
                        } else {
                            target.getAndSet(tuple.getValue());
                        }
                    } else if (quota instanceof LongAdder) {
                        // get quota long adder
                        LongAdder target = (LongAdder) quota;

                        if (tuple.isCresc()) {
                            target.add(tuple.getValue());
                        } else {
                            // reset 0
                            target.reset();
                            // 0 + value
                            target.add(tuple.getValue());
                        }
                    }
                } catch (Exception e) {
                    Logger.log().error("record metrics has an exception: {}.", e.getMessage());
                }
            }

            return null;
        });
    }

    private MetricsQuadTuple sessionTuple(String clientId, long point, long value) {
        if (!IdUtil.validateClientId(clientId)) {
            return null;
        }

        MetricsQuadTuple tuple = null;

        if (MetricsConst.PN_SSM_TOPIC_COUNT == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SSM_TOPIC_COUNT_PREFIX + clientId, value, true);
        } else if (MetricsConst.PN_SSM_AWAIT_SIZE == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SSM_AWAIT_SIZE_PREFIX + clientId, value, false);
        } else if (MetricsConst.PN_SSM_QUEUE_SIZE == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SSM_QUEUE_SIZE_PREFIX + clientId, value, false);
        } else if (MetricsConst.PN_SSM_INFLIGHT_SIZE == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SSM_INFLIGHT_SIZE_PREFIX + clientId, value, false);
        } else if (MetricsConst.PN_SSM_DROPPED_COUNT == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SSM_DROPPED_COUNT_PREFIX + clientId, value, true);
        } else if (MetricsConst.PN_SSM_SENT_COUNT == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SSM_SENT_COUNT_PREFIX + clientId, value, true);
        } else if (MetricsConst.PN_SSM_ENQUEUE_COUNT == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SSM_ENQUEUE_COUNT_PREFIX + clientId, value, true);
        }

        return tuple;
    }

    private MetricsQuadTuple nodeTuple(long point, long value) {
        MetricsQuadTuple tuple = null;

        if (MetricsConst.PN_SM_CONN_COUNT == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getConnCount(), value, true);
        } else if (MetricsConst.PN_SM_CONN_MAX == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getConnMax(), value, false, t -> {
                // get connection count
                long count = NodeMetrics.getInstance().getConnCount().sum();
                // get connection max
                long max = NodeMetrics.getInstance().getConnMax().sum();

                if (count > max) {
                    // set new max value
                    t.setValue(count);
                    // return true, metrics service will update the quota
                    return true;
                }

                return false;
            });
        } else if (MetricsConst.PN_SM_TOPIC_COUNT == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getTopicCount(), value, false, t -> {
                // set new topic count
                t.setValue(cacheService.countTopics(cacheService.localNodeId(), null));
                // return true, metrics service will update the quota
                return true;
            });
        } else if (MetricsConst.PN_SM_TOPIC_MAX == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getTopicMax(), value, false, t -> {
                // get topic count
                long count = NodeMetrics.getInstance().getTopicCount().sum();
                // get topic max
                long max = NodeMetrics.getInstance().getTopicMax().sum();

                if (count > max) {
                    // set new max value
                    t.setValue(count);
                    // return true, metrics service will update the quota
                    return true;
                }

                return false;
            });
        } else if (MetricsConst.PN_SM_SESS_COUNT == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getSessCount(), value, true);
        } else if (MetricsConst.PN_SM_SESS_MAX == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getSessMax(), value, false, t -> {
                // get session count
                long count = NodeMetrics.getInstance().getSessCount().sum();
                // get session max
                long max = NodeMetrics.getInstance().getSessMax().sum();

                if (count > max) {
                    // set new max value
                    t.setValue(count);
                    // return true, metrics service will update the quota
                    return true;
                }

                return false;
            });
        } else if (MetricsConst.PN_SM_SUB_COUNT == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getSubCount(), value, false, t -> {
                // set new topic count
                t.setValue(cacheService.countSubscribes(cacheService.localNodeId(), null));
                // return true, metrics service will update the quota
                return true;
            });
        } else if (MetricsConst.PN_SM_SUB_MAX == point) {
            tuple = new MetricsQuadTuple(NodeMetrics.getInstance().getSubMax(), value, false, t -> {
                // get subscribe count
                long count = NodeMetrics.getInstance().getSubCount().sum();
                // get subscribe max
                long max = NodeMetrics.getInstance().getSubMax().sum();

                if (count > max) {
                    // set new max value
                    t.setValue(count);
                    // return true, metrics service will update the quota
                    return true;
                }

                return false;
            });
        }

        return tuple;
    }

    private MetricsQuadTuple packetTuple(long point, long value) {
        MetricsQuadTuple tuple = null;

        if (MetricsConst.PN_SM_PACKET_CONNECT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketConnect(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_CONNACK == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketConnack(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_DISCONNECT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketDisconnect(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PINGREQ == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPingreq(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PINGRESP == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPingresp(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBLISH_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPublishReceived(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBLISH_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPublishSent(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBACK_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubackReceived(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBACK_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubackSent(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBACK_MISSED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubackMissed(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBREC_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubrecReceived(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBREC_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubrecSent(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBREC_MISSED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubrecMissed(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBREL_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubrelReceived(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBREL_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubrelSent(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBREL_MISSED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubrelMissed(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBCOMP_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubcompReceived(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBCOMP_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubcompSent(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_PUBCOMP_MISSED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketPubcompMissed(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_SUBSCRIBE == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketSubscribe(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_SUBACK == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketSuback(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_UNSUBSCRIBE == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketUnsubscribe(), value, true);
        } else if (MetricsConst.PN_SM_PACKET_UNSUBACK == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getPacketUnsuback(), value, true);
        }

        return tuple;
    }

    private MetricsQuadTuple messageTuple(long point, long value) {
        MetricsQuadTuple tuple = null;

        if (MetricsConst.PN_SM_MESSAGE_DROPPED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getMessageDropped(), value, true);
        } else if (MetricsConst.PN_SM_MESSAGE_QOS0_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getMessageQos0Received(), value, true);
        } else if (MetricsConst.PN_SM_MESSAGE_QOS0_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getMessageQos0Sent(), value, true);
        } else if (MetricsConst.PN_SM_MESSAGE_QOS1_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getMessageQos1Received(), value, true);
        } else if (MetricsConst.PN_SM_MESSAGE_QOS1_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getMessageQos1Sent(), value, true);
        } else if (MetricsConst.PN_SM_MESSAGE_QOS2_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getMessageQos2Received(), value, true);
        } else if (MetricsConst.PN_SM_MESSAGE_QOS2_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getMessageQos2Sent(), value, true);
        }

        return tuple;
    }

    private MetricsQuadTuple byteTuple(long point, long value) {
        MetricsQuadTuple tuple = null;

        if (MetricsConst.PN_SM_BYTE_RECEIVED == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getByteReceived(), value, true);
        } else if (MetricsConst.PN_SM_BYTE_SENT == point) {
            tuple = new MetricsQuadTuple(MqttMetrics.getInstance().getByteSent(), value, true);
        }

        return tuple;
    }

    private MetricsQuadTuple retainTuple(long point, long value) {
        MetricsQuadTuple tuple = null;

        if (MetricsConst.PN_SM_RETAIN_COUNT == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SM_RETAIN_COUNT, value, true);
        } else if (MetricsConst.PN_SM_RETAIN_MAX == point) {
            tuple = new MetricsQuadTuple(MetricsConst.SM_RETAIN_MAX, value, false, t -> {
                // get retain count
                IgniteAtomicLong retainCount = cacheService.getIgnite().atomicLong(MetricsConst.SM_RETAIN_COUNT, 0, false);
                // get retain max
                IgniteAtomicLong retainMax = cacheService.getIgnite().atomicLong(MetricsConst.SM_RETAIN_MAX, 0, false);

                if (retainCount != null && !retainCount.removed() && retainMax != null && !retainMax.removed()) {
                    // get count value
                    long count = retainCount.get();
                    // get max value
                    long max = retainMax.get();

                    if (count > max) {
                        // set new max value
                        t.setValue(count);
                        // return true, metrics service will update the quota
                        return true;
                    }
                }

                return false;
            });
        }

        return tuple;
    }

    private <T> Future<T> submit(Callable<T> task) {
        return executorService.submit(task);
    }

}
