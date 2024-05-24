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

package io.stuart.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.hutool.core.util.ReflectUtil;
import io.stuart.consts.MetricsConst;

public class MetricsUtil {

    private static final List<String> sessionQuotas;

    private static final Map<Long, List<Long>> groupedQuotas;

    static {
        // initialize session quotas
        sessionQuotas = sessionQuotas(MetricsConst.SSM_PREFIX);

        // initialize grouped quotas
        groupedQuotas = groupedQuotas();
    }

    public static List<String> getSessionQuotas() {
        return sessionQuotas;
    }

    public static Map<Long, List<Long>> getGroupedQuotas() {
        return groupedQuotas;
    }

    private static List<String> sessionQuotas(String... prefixes) {
        // session quotas
        List<String> result = new ArrayList<>();

        // get class fields
        Field[] fields = ReflectUtil.getFields(MetricsConst.class);
        // field string value
        String value = null;

        for (Field field : fields) {
            // get field string value
            value = ReflectUtil.getFieldValue(MetricsConst.class, field).toString();

            if (prefixes == null || prefixes.length == 0) {
                // add value to result
                result.add(value);
            } else {
                for (String prefix : prefixes) {
                    // start with prefix
                    if (!value.equals(prefix) && value.startsWith(prefix)) {
                        // add value to result
                        result.add(value);

                        // break;
                        break;
                    }
                }
            }
        }

        return result;
    }

    private static Map<Long, List<Long>> groupedQuotas() {
        // grouped quotas
        Map<Long, List<Long>> result = new HashMap<>();

        List<Long> connection = new ArrayList<>();
        connection.add(MetricsConst.PN_SM_CONN_COUNT);
        connection.add(MetricsConst.PN_SM_CONN_MAX);

        List<Long> session = new ArrayList<>();
        session.add(MetricsConst.PN_SM_SESS_COUNT);
        session.add(MetricsConst.PN_SM_SESS_MAX);

        List<Long> topicAndSubscribe = new ArrayList<>();
        topicAndSubscribe.add(MetricsConst.PN_SM_TOPIC_COUNT);
        topicAndSubscribe.add(MetricsConst.PN_SM_TOPIC_MAX);
        topicAndSubscribe.add(MetricsConst.PN_SM_SUB_COUNT);
        topicAndSubscribe.add(MetricsConst.PN_SM_SUB_MAX);

        List<Long> qos0Received = new ArrayList<>();
        qos0Received.add(MetricsConst.PN_SM_PACKET_PUBLISH_RECEIVED);
        qos0Received.add(MetricsConst.PN_SM_MESSAGE_QOS0_RECEIVED);
        qos0Received.add(MetricsConst.PN_SM_BYTE_RECEIVED);

        List<Long> qos1Received = new ArrayList<>();
        qos1Received.add(MetricsConst.PN_SM_PACKET_PUBLISH_RECEIVED);
        qos1Received.add(MetricsConst.PN_SM_PACKET_PUBACK_SENT);
        qos1Received.add(MetricsConst.PN_SM_MESSAGE_QOS1_RECEIVED);
        qos1Received.add(MetricsConst.PN_SM_BYTE_RECEIVED);

        List<Long> qos2Received = new ArrayList<>();
        qos2Received.add(MetricsConst.PN_SM_PACKET_PUBLISH_RECEIVED);
        qos2Received.add(MetricsConst.PN_SM_PACKET_PUBREC_SENT);
        qos2Received.add(MetricsConst.PN_SM_MESSAGE_QOS2_RECEIVED);
        qos2Received.add(MetricsConst.PN_SM_BYTE_RECEIVED);

        List<Long> qos0Sent = new ArrayList<>();
        qos0Sent.add(MetricsConst.PN_SSM_SENT_COUNT);
        qos0Sent.add(MetricsConst.PN_SM_PACKET_PUBLISH_SENT);
        qos0Sent.add(MetricsConst.PN_SM_MESSAGE_QOS0_SENT);
        qos0Sent.add(MetricsConst.PN_SM_BYTE_SENT);

        List<Long> qos1Sent = new ArrayList<>();
        qos1Sent.add(MetricsConst.PN_SSM_SENT_COUNT);
        qos1Sent.add(MetricsConst.PN_SM_PACKET_PUBLISH_SENT);
        qos1Sent.add(MetricsConst.PN_SM_MESSAGE_QOS1_SENT);
        qos1Sent.add(MetricsConst.PN_SM_BYTE_SENT);

        List<Long> qos2Sent = new ArrayList<>();
        qos2Sent.add(MetricsConst.PN_SSM_SENT_COUNT);
        qos2Sent.add(MetricsConst.PN_SM_PACKET_PUBLISH_SENT);
        qos2Sent.add(MetricsConst.PN_SM_MESSAGE_QOS2_SENT);
        qos2Sent.add(MetricsConst.PN_SM_BYTE_SENT);

        List<Long> messageDropped = new ArrayList<>();
        messageDropped.add(MetricsConst.PN_SSM_DROPPED_COUNT);
        messageDropped.add(MetricsConst.PN_SM_MESSAGE_DROPPED);

        List<Long> messageEnqueueAndDropped = new ArrayList<>();
        messageEnqueueAndDropped.add(MetricsConst.PN_SSM_ENQUEUE_COUNT);
        messageEnqueueAndDropped.add(MetricsConst.PN_SSM_DROPPED_COUNT);
        messageEnqueueAndDropped.add(MetricsConst.PN_SM_MESSAGE_DROPPED);

        result.put(MetricsConst.GPN_CONNECTION, connection);
        result.put(MetricsConst.GPN_SESSION, session);
        result.put(MetricsConst.GPN_TOPIC_AND_SUBSCRIBE, topicAndSubscribe);
        result.put(MetricsConst.GPN_QOS0_RECEIVED, qos0Received);
        result.put(MetricsConst.GPN_QOS1_RECEIVED, qos1Received);
        result.put(MetricsConst.GPN_QOS2_RECEIVED, qos2Received);
        result.put(MetricsConst.GPN_QOS0_SENT, qos0Sent);
        result.put(MetricsConst.GPN_QOS1_SENT, qos1Sent);
        result.put(MetricsConst.GPN_QOS2_SENT, qos2Sent);
        result.put(MetricsConst.GPN_MESSAGE_DROPPED, messageDropped);
        result.put(MetricsConst.GPN_MESSAGE_ENQUEUE_AND_DROPPED, messageEnqueueAndDropped);

        return result;
    }

}
