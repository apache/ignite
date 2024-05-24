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

// jquery select2
(function() {
    if (jQuery && jQuery.fn && jQuery.fn.select2 && jQuery.fn.select2.amd) {
        var e = jQuery.fn.select2.amd;

        return e.define("select2/i18n/zh", [], function() {
            return {
                errorLoading: function() { return "无法载入结果。" },
                inputTooLong: function(e) {
                    var t = e.input.length - e.maximum,
                        n = "请删除" + t + "个字符";
                    return n
                },
                inputTooShort: function(e) {
                    var t = e.minimum - e.input.length,
                        n = "请再输入至少" + t + "个字符";
                    return n
                },
                loadingMore: function() { return "载入更多结果…" },
                maximumSelected: function(e) { var t = "最多只能选择" + e.maximum + "个项目"; return t },
                noResults: function() { return "未找到结果" },
                searching: function() { return "搜索中…" }
            }
        }), { define: e.define, require: e.require }
    }
})();

// jquery validation
(function() {
    if ($ && $.validator && $.validator.messages) {
        $.extend($.validator.messages, {
            required: "这是必填字段",
            remote: "请修正此字段",
            email: "请输入有效的电子邮件地址",
            url: "请输入有效的网址",
            date: "请输入有效的日期",
            dateISO: "请输入有效的日期 (YYYY-MM-DD)",
            number: "请输入有效的数字",
            digits: "只能输入数字",
            equalTo: "你的输入不相同",
            extension: "请输入有效的后缀",
            maxlength: $.validator.format("最多可以输入 {0} 个字符"),
            minlength: $.validator.format("最少要输入 {0} 个字符"),
            rangelength: $.validator.format("请输入长度在 {0} 到 {1} 之间的字符串"),
            range: $.validator.format("请输入范围在 {0} 到 {1} 之间的数值"),
            max: $.validator.format("请输入不大于 {0} 的数值"),
            min: $.validator.format("请输入不小于 {0} 的数值")
        });
    }
})();

// stuart system
(function() {
    var messages_zh_cn = {
        datatables_processing: '正在获取数据，请稍后！',
        datatables_zeroRecords: '未获取到数据',
        datatables_info: '从 第 _START_ 条 到 第 _END_ 条 共 _TOTAL_ 条记录',
        datatables_infoEmpty: '共 0 条记录',
        datatables_paginate_first: '第一页',
        datatables_paginate_previous: '上一页',
        datatables_paginate_next: '下一页',
        datatables_paginate_last: '最后一页',
        datatables_search: '在表格中搜索：',

        menu_monitor: '监控',
        menu_management: '管理',
        menu_tool: '工具',
        menu_system: '系统',
        menu_monitor_console: '控制台',
        menu_monitor_connect: '连接',
        menu_monitor_session: '会话',
        menu_monitor_topic: '主题',
        menu_monitor_subscribe: '订阅',
        menu_management_user: '连接用户',
        menu_management_acl: '访问控制',
        menu_management_listener: '监听器',
        menu_tool_websocket: 'Websocket',
        menu_tool_restful: 'Restful接口',
        menu_system_admin: '系统管理员',

        common_node: '节点',
        common_client_id: '客户端ID',
        common_clean_session: '清除会话',
        common_keep_alive: '心跳(秒)',
        common_sub: '订阅',
        common_pub: '发布',
        common_qos: '服务质量',
        common_topic: '主题',
        common_message: '消息',
        common_username: '用户名',
        common_password: '密码',
        common_ip_address: 'IP地址',
        common_host: '主机地址',
        common_port: '端口号',
        common_oper: '操作',
        common_btn_processing: '处理中',
        common_alert_title: '系统提示',
        common_alert_error_content: '系统异常，请联系Stuart开发工作室！',
        common_login_btn: '登录',
        common_search_btn: '查询',
        common_add_btn: '添加',
        common_edit_btn: '编辑',
        common_delete_btn: '删除',
        common_cancel_btn: '取消',
        common_save_btn: '保存',
        common_confirm_btn: '确定',
        common_close_btn: '关闭',
        common_question_mark: '？',
        common_yes: '是',
        common_no: '否',
        common_swtich_cond_required: '这是必填字段',
        common_passwd_equal: '两次输入的密码不一致',
        common_passwd_not_equal: '新密码不能与旧密码相同',
        common_passwd_error: '密码错误',

        login_message: '登录并开始管理服务器',
        login_username: '用户名',
        login_username_required: '请输入用户名',
        login_password: '密码',
        login_password_required: '请输入密码',
        login_remember_me: '记住我',
        login_username_password_error: '用户名或密码错误',

        console_system_info: '系统信息',
        console_name_label: '名称',
        console_name_desc: 'Stuart MQTT 服务器',
        console_version_label: '版本',
        console_uptime_label: '运行时间',
        console_systime_label: '系统时间',
        console_nodes: '节点信息',
        console_java_version: 'Java版本',
        console_thread: '线程<br>(run/peak)',
        console_cpu: 'CPU<br>(1load/5load/15load)',
        console_heap: '堆内存<br>(used/total)',
        console_off_heap: '堆外内存<br>(used/total)',
        console_max_fds: '最大文件句柄数',
        console_status: '状态',
        console_stats: '运行统计',
        console_stats_conn_count: '连接数',
        console_stats_conn_max: '连接峰值',
        console_stats_topic_count: '主题数',
        console_stats_topic_max: '主题峰值',
        console_stats_retain_count: '保留消息数',
        console_stats_retain_max: '保留消息最大数',
        console_stats_sess_count: '会话数',
        console_stats_sess_max: '会话峰值',
        console_stats_sub_count: '订阅数',
        console_stats_sub_max: '订阅峰值',
        console_metrics: '度量指标',
        console_metrics_packets: 'MQTT报文',
        console_metrics_messages: '消息(个数)',
        console_metrics_bytes: '消息流量(字节)',

        connect_protocol_version: '协议版本',
        connect_connect_time: '连接时间',

        session_subscriptions: '订阅数',
        session_max_inflight: '最大拥塞',
        session_inflight_size: '当前拥塞',
        session_queue_size: '当前缓存消息',
        session_droppeds: '丢弃消息',
        session_await_rels: '等待释放',
        session_delivers: '投递消息',
        session_enqueues: '入队消息',
        session_create_time: '创建时间',

        user_username: '连接用户名',
        user_desc: '备注',
        user_passwd: '密码',
        user_passwd_conf: '确认密码',
        user_passwd_change: '修改密码',
        user_admin_passwd: '当前管理员密码',
        user_passwd_new: '新密码',
        user_passwd_new_conf: '确认新密码',
        user_add_title: '添加连接用户',
        user_edit_title: '编辑连接用户',
        user_delete_title: '删除连接用户',
        user_existed_message: '连接用户名已存在',
        user_delete_conf_text: '确定删除连接用户：',
        user_add_succeeded: '添加连接用户成功',
        user_add_failed: '添加连接用户失败',
        user_edit_succeeded: '编辑连接用户成功',
        user_edit_failed: '编辑连接用户失败',
        user_delete_succeeded: '删除连接用户成功',
        user_delete_failed: '删除连接用户失败',

        acl_explain: '说明',
        acl_explain_detail_one: '1.如果主题中包含“+”或“#”等通配符，默认情况下系统会按照通配符进行匹配比较；',
        acl_explain_detail_two: '2.如果需要主题进行相等匹配，请使用eq表达式，例如：eq(/hello/world/+)。',
        acl_seq: '序号',
        acl_target: '控制目标',
        acl_type: '目标类型',
        acl_topic: '主题',
        acl_access: '访问类型',
        acl_authority: '访问权限',
        acl_item: '访问控制项',
        acl_connect_user: '连接用户',
        acl_ip_addr: 'IP地址',
        acl_client_id: '客户端ID',
        acl_all: '所有',
        acl_sub: '订阅',
        acl_pub: '发布',
        acl_subpub: '订阅发布',
        acl_deny: '禁止',
        acl_allow: '允许',
        acl_delete_target: '控制目标：',
        acl_delete_type: '目标类型：',
        acl_delete_topic: '主题：',
        acl_delete_access: '访问类型：',
        acl_delete_authority: '访问权限：',
        acl_add_title: '添加访问控制项',
        acl_edit_title: '编辑访问控制项',
        acl_delete_title: '删除访问控制项',
        acl_delete_conf_text: '确定删除访问控制项？',
        acl_add_succeeded: '添加访问控制项成功',
        acl_add_failed: '添加访问控制项失败',
        acl_edit_succeeded: '编辑访问控制项成功',
        acl_edit_failed: '编辑访问控制项失败',
        acl_delete_succeeded: '删除访问控制项成功',
        acl_delete_failed: '删除访问控制项失败',

        listener_protocol: '协议',
        listener_address_port: '监听地址',
        listener_conn_max_limit: '最大客户端连接数量',
        listener_conn_count: '当前客户端连接数量',

        ws_conn_title: '连接',
        ws_conn_path: 'Path',
        ws_conn_ssl: 'SSL',
        ws_conn_status: '连接状态：',
        ws_conn_status_connected: '已连接',
        ws_conn_status_disconnected: '未连接',
        ws_sub_title: '订阅',
        ws_sub_list: '订阅列表',
        ws_sub_time: '时间',
        ws_msg_title: '消息',
        ws_msg_retained: '保留',
        ws_msg_pub_list: '发布消息列表',
        ws_msg_sub_list: '订阅消息列表',
        ws_msg_time: '时间',
        ws_connect_btn: '连接',
        ws_disconnect_btn: '断开连接',
        ws_subscribe_btn: '订阅',
        ws_send_btn: '发送',
        ws_notify_connect_succeed: '连接成功',
        ws_notify_sub_succeed: '订阅成功',
        ws_notify_unsub_succeed: '取消订阅成功',
        ws_notify_send_succeed: '发送成功',

        admin_account: '管理员账号',
        admin_desc: '备注',
        admin_passwd: '密码',
        admin_passwd_conf: '确认密码',
        admin_passwd_change: '修改密码',
        admin_passwd_old: '旧密码',
        admin_passwd_new: '新密码',
        admin_passwd_new_conf: '确认新密码',
        admin_add_title: '添加管理员',
        admin_edit_title: '编辑管理员',
        admin_delete_title: '删除管理员',
        admin_existed_message: '账号已存在',
        admin_delete_conf_text: '确定删除管理员：',
        admin_add_succeeded: '添加管理员成功',
        admin_add_failed: '添加管理员失败',
        admin_edit_succeeded: '编辑管理员成功',
        admin_edit_failed: '编辑管理员失败',
        admin_delete_succeeded: '删除管理员成功',
        admin_delete_failed: '删除管理员失败'
    };

    $.extend(locale_messages, messages_zh_cn);
})();