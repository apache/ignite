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

public interface CacheConst {

    static final String MEM_DATA_REGION_NAME = "stuart_mem_data_region";

    static final String NODE_NAME = "stuart_nodes";

    static final String LISTENER_NAME = "stuart_listeners";

    static final String CONNECTION_NAME = "stuart_connections";

    static final String SESSION_NAME = "stuart_sessions";

    static final String ROUTER_NAME = "stuart_routers";

    static final String TRIE_NAME = "stuart_tries";

    static final String CLIENT_TOPIC_SET_NAME = "stuart_client_topic_sets";

    static final String TOPIC_CLIENT_SET_NAME = "stuart_topic_client_sets";

    static final String AWAIT_MESSAGE_NAME = "stuart_await_messages";

    static final String AWAIT_SET_PREFIX = "stuart_await_set_";

    static final String QUEUE_PREFIX = "stuart_queue_";

    static final String INFLIGHT_MESSAGE_NAME = "stuart_inflight_messages";

    static final String INFLIGHT_SET_PREFIX = "stuart_inflight_set_";

    static final String RETAIN_NAME = "stuart_retains";

    static final String WILL_NAME = "stuart_wills";

    static final String USER_NAME = "stuart_users";

    static final String ACL_NAME = "stuart_acls";

    static final String ACL_SEQ = "stuart_acl_seq";

    static final String ADMIN_NAME = "stuart_admins";

    static final String CLS_PUBLISH_TOPIC = "stuart_cls_publish";

    static final String CLS_KICK_TOPIC = "stuart_cls_kick";

    static final String MESSAGE_EXPIRER_VALUE = "";

    static final String SYS_AES_KEY = ParamConst.AUTH_AES_KEY;

    static final String SYS_ADMIN_ACCOUNT = "admin";

    static final String SYS_ADMIN_PASSWORD = "stuart";

    static final int DEF_CACHE_BACKUPS = 0;

    static final int DEF_SET_BACKUPS = 1;

    static final long DEF_SET_OFFHEAP_MAX_MEMORY = 0;

    static final int DEF_QUEUE_BACKUPS = 1;

    static final long DEF_QUEUE_OFFHEAP_MAX_MEMORY = 0;

    static final Integer ERROR = -99;

    static final Integer EXISTED = -1;

    static final Integer NONE = 0;

    static final Integer SUCCEEDED = 1;

    static final Integer OLD = -1;

    static final Integer NEW = 1;

    static final Integer DEF_PAGE_NUM = 1;

    static final Integer DEF_PAGE_SIZE = 10;

}
