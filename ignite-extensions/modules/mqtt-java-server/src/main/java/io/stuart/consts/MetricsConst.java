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

package io.stuart.consts;

public interface MetricsConst {

    static String SSM_PREFIX = "stuart_ssm_";

    static String SSM_TOPIC_COUNT_PREFIX = SSM_PREFIX + "topicCount_";

    static String SSM_AWAIT_SIZE_PREFIX = SSM_PREFIX + "awaitSize_";

    static String SSM_QUEUE_SIZE_PREFIX = SSM_PREFIX + "queueSize_";

    static String SSM_INFLIGHT_SIZE_PREFIX = SSM_PREFIX + "inflightSize_";

    static String SSM_DROPPED_COUNT_PREFIX = SSM_PREFIX + "droppedCount_";

    static String SSM_SENT_COUNT_PREFIX = SSM_PREFIX + "sentCount_";

    static String SSM_ENQUEUE_COUNT_PREFIX = SSM_PREFIX + "enqueueCount_";

    static String SM_RETAIN_COUNT = "stuart_sm_retainCount";

    static String SM_RETAIN_MAX = "stuart_sm_retainMax";

    static long PN_SSM_TOPIC_COUNT = 0;

    static long PN_SSM_AWAIT_SIZE = 1;

    static long PN_SSM_QUEUE_SIZE = 2;

    static long PN_SSM_INFLIGHT_SIZE = 3;

    static long PN_SSM_DROPPED_COUNT = 4;

    static long PN_SSM_SENT_COUNT = 5;

    static long PN_SSM_ENQUEUE_COUNT = 6;

    static long PN_SM_CONN_COUNT = 100;

    static long PN_SM_CONN_MAX = 101;

    static long PN_SM_TOPIC_COUNT = 102;

    static long PN_SM_TOPIC_MAX = 103;

    static long PN_SM_SESS_COUNT = 104;

    static long PN_SM_SESS_MAX = 105;

    static long PN_SM_SUB_COUNT = 106;

    static long PN_SM_SUB_MAX = 107;

    static long PN_SM_PACKET_CONNECT = 200;

    static long PN_SM_PACKET_CONNACK = 201;

    static long PN_SM_PACKET_DISCONNECT = 202;

    static long PN_SM_PACKET_PINGREQ = 203;

    static long PN_SM_PACKET_PINGRESP = 204;

    static long PN_SM_PACKET_PUBLISH_RECEIVED = 205;

    static long PN_SM_PACKET_PUBLISH_SENT = 206;

    static long PN_SM_PACKET_PUBACK_RECEIVED = 207;

    static long PN_SM_PACKET_PUBACK_SENT = 208;

    static long PN_SM_PACKET_PUBACK_MISSED = 209;

    static long PN_SM_PACKET_PUBREC_RECEIVED = 210;

    static long PN_SM_PACKET_PUBREC_SENT = 211;

    static long PN_SM_PACKET_PUBREC_MISSED = 212;

    static long PN_SM_PACKET_PUBREL_RECEIVED = 213;

    static long PN_SM_PACKET_PUBREL_SENT = 214;

    static long PN_SM_PACKET_PUBREL_MISSED = 215;

    static long PN_SM_PACKET_PUBCOMP_RECEIVED = 216;

    static long PN_SM_PACKET_PUBCOMP_SENT = 217;

    static long PN_SM_PACKET_PUBCOMP_MISSED = 218;

    static long PN_SM_PACKET_SUBSCRIBE = 219;

    static long PN_SM_PACKET_SUBACK = 220;

    static long PN_SM_PACKET_UNSUBSCRIBE = 221;

    static long PN_SM_PACKET_UNSUBACK = 222;

    static long PN_SM_MESSAGE_DROPPED = 300;

    static long PN_SM_MESSAGE_QOS0_RECEIVED = 301;

    static long PN_SM_MESSAGE_QOS0_SENT = 302;

    static long PN_SM_MESSAGE_QOS1_RECEIVED = 303;

    static long PN_SM_MESSAGE_QOS1_SENT = 304;

    static long PN_SM_MESSAGE_QOS2_RECEIVED = 305;

    static long PN_SM_MESSAGE_QOS2_SENT = 306;

    static long PN_SM_BYTE_RECEIVED = 400;

    static long PN_SM_BYTE_SENT = 401;

    static long PN_SM_RETAIN_COUNT = 500;

    static long PN_SM_RETAIN_MAX = 501;

    static long GPN_CONNECTION = 600;

    static long GPN_SESSION = 601;

    static long GPN_TOPIC_AND_SUBSCRIBE = 602;

    static long GPN_MESSAGE_RECEIVED = 603;

    static long GPN_MESSAGE_SENT = 604;

    static long GPN_MESSAGE_DROPPED = 605;

    static long GPN_MESSAGE_ENQUEUE_AND_DROPPED = 606;

    static long GPN_QOS0_RECEIVED = 700;

    static long GPN_QOS1_RECEIVED = 701;

    static long GPN_QOS2_RECEIVED = 702;

    static long GPN_QOS0_SENT = 703;

    static long GPN_QOS1_SENT = 704;

    static long GPN_QOS2_SENT = 705;

}
