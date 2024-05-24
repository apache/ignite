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

        return e.define("select2/i18n/en", [], function() {
            return {
                errorLoading: function() { return "The results could not be loaded." },
                inputTooLong: function(e) {
                    var t = e.input.length - e.maximum,
                        n = "Please delete " + t + " character";
                    return t != 1 && (n += "s"), n
                },
                inputTooShort: function(e) {
                    var t = e.minimum - e.input.length,
                        n = "Please enter " + t + " or more characters";
                    return n
                },
                loadingMore: function() { return "Loading more results…" },
                maximumSelected: function(e) { var t = "You can only select " + e.maximum + " item"; return e.maximum != 1 && (t += "s"), t },
                noResults: function() { return "No results found" },
                searching: function() { return "Searching…" }
            }
        }), { define: e.define, require: e.require }
    }
})();

// jquery validation
(function() {
    if ($ && $.validator && $.validator.messages) {
        $.extend($.validator.messages, {
            required: "This field is required.",
            remote: "Please fix this field.",
            email: "Please enter a valid email address.",
            url: "Please enter a valid URL.",
            date: "Please enter a valid date.",
            dateISO: "Please enter a valid date (ISO).",
            number: "Please enter a valid number.",
            digits: "Please enter only digits.",
            equalTo: "Please enter the same value again.",
            extension: "Please enter the valid extension.",
            maxlength: $.validator.format("Please enter no more than {0} characters."),
            minlength: $.validator.format("Please enter at least {0} characters."),
            rangelength: $.validator.format("Please enter a value between {0} and {1} characters long."),
            range: $.validator.format("Please enter a value between {0} and {1}."),
            max: $.validator.format("Please enter a value less than or equal to {0}."),
            min: $.validator.format("Please enter a value greater than or equal to {0}.")
        });
    }
})();

// stuart system
(function() {
    var messages_en_us = {
        datatables_processing: 'Processing...',
        datatables_zeroRecords: 'No data available in table',
        datatables_info: 'Showing _START_ to _END_ of _TOTAL_ entries',
        datatables_infoEmpty: 'Showing 0 to 0 of 0 entries',
        datatables_paginate_first: 'First',
        datatables_paginate_previous: 'Previous',
        datatables_paginate_next: 'Next',
        datatables_paginate_last: 'Last',
        datatables_search: 'Search:',

        menu_monitor: 'Monitor',
        menu_management: 'Management',
        menu_tool: 'Tool',
        menu_system: 'System',
        menu_monitor_console: 'Console',
        menu_monitor_connect: 'Connect',
        menu_monitor_session: 'Session',
        menu_monitor_topic: 'Topic',
        menu_monitor_subscribe: 'Subscribe',
        menu_management_user: 'Connect User',
        menu_management_acl: 'Access Control',
        menu_management_listener: 'Listener',
        menu_tool_websocket: 'Websocket',
        menu_tool_restful: 'Restful API',
        menu_system_admin: 'Administrator',

        common_node: 'Node',
        common_client_id: 'Client ID',
        common_clean_session: 'Clean Session',
        common_keep_alive: 'Keep Alive(Sec.)',
        common_sub: 'Subscribe',
        common_pub: 'Publish',
        common_qos: 'QoS',
        common_topic: 'Topic',
        common_message: 'Message',
        common_username: 'Username',
        common_password: 'Password',
        common_ip_address: 'IP Address',
        common_host: 'Host',
        common_port: 'Port',
        common_oper: 'Operator',
        common_btn_processing: 'Processing',
        common_alert_title: 'Prompt',
        common_alert_error_content: 'System exception, please contact the Stuart Studio!',
        common_login_btn: 'Login',
        common_search_btn: 'Search',
        common_add_btn: 'Add',
        common_edit_btn: 'Edit',
        common_delete_btn: 'Delete',
        common_cancel_btn: 'Cancel',
        common_save_btn: 'Save',
        common_confirm_btn: 'Confirm',
        common_close_btn: 'Close',
        common_question_mark: '?',
        common_yes: 'Yes',
        common_no: 'No',
        common_swtich_cond_required: 'This field is required.',
        common_passwd_equal: 'Please enter the same password again.',
        common_passwd_not_equal: 'The new password cannot be the same as the old one.',
        common_passwd_error: 'Password is incorrect.',

        login_message: 'Login and Manage the Stuart Server.',
        login_username: 'Username',
        login_username_required: 'Username is required.',
        login_password: 'Password',
        login_password_required: 'Password is required.',
        login_remember_me: 'Remember me',
        login_username_password_error: 'No such username, or password incorrect.',

        console_system_info: 'System',
        console_name_label: 'Name',
        console_name_desc: 'Stuart MQTT Server',
        console_version_label: 'Version',
        console_uptime_label: 'Uptime',
        console_systime_label: 'System Time',
        console_nodes: 'Node Informations',
        console_java_version: 'Java Version',
        console_thread: 'Thread<br>(run/peak)',
        console_cpu: 'CPU<br>(1load/5load/15load)',
        console_heap: 'Heap Mem.<br>(used/total)',
        console_off_heap: 'Off-Heap Mem.<br>(used/total)',
        console_max_fds: 'Max FDS',
        console_status: 'Status',
        console_stats: 'Statistics',
        console_stats_conn_count: 'Conn. Cnt',
        console_stats_conn_max: 'Conn. Max',
        console_stats_topic_count: 'Topic Cnt',
        console_stats_topic_max: 'Topic Max',
        console_stats_retain_count: 'Retain Cnt',
        console_stats_retain_max: 'Retain Max',
        console_stats_sess_count: 'Sess. Cnt',
        console_stats_sess_max: 'Sess. Max',
        console_stats_sub_count: 'Sub. Cnt',
        console_stats_sub_max: 'Sub. Max',
        console_metrics: 'Metrics',
        console_metrics_packets: 'Packets',
        console_metrics_messages: 'Messages',
        console_metrics_bytes: 'Message Bytes',

        connect_protocol_version: 'Protocol Version',
        connect_connect_time: 'Connect Time',

        session_subscriptions: 'Subs',
        session_max_inflight: 'Max Inflight',
        session_inflight_size: 'Inflight Size',
        session_queue_size: 'Queue Size',
        session_droppeds: 'Droppeds',
        session_await_rels: 'Await Rels',
        session_delivers: 'Delivers',
        session_enqueues: 'Enqueues',
        session_create_time: 'Create Time',

        user_username: 'Connect Username',
        user_desc: 'Comment',
        user_passwd: 'Password',
        user_passwd_conf: 'Confirm Password',
        user_passwd_change: 'Change Password',
        user_admin_passwd: 'Administrator Password',
        user_passwd_new: 'New Password',
        user_passwd_new_conf: 'Confirm New Password',
        user_add_title: 'Add Connect User',
        user_edit_title: 'Edit Connect User',
        user_delete_title: 'Delete Connect User',
        user_existed_message: 'Connect username is existed',
        user_delete_conf_text: 'Whether you want to delete the connect user: ',
        user_add_succeeded: 'Add connect user succeeded.',
        user_add_failed: 'Add connect user failed.',
        user_edit_succeeded: 'Edit connect user succeeded.',
        user_edit_failed: 'Edit connect user failed.',
        user_delete_succeeded: 'Delete connect user succeeded.',
        user_delete_failed: 'Delete connect user failed.',

        acl_explain: 'Explain',
        acl_explain_detail_one: '1.If a topic contains wildcards such as + or #, the system matches them by default;',
        acl_explain_detail_two: '2.If the topic needs to be matched equally, use the eq expression, for example: eq(/hello/world/+).',
        acl_seq: 'Seq',
        acl_target: 'Target',
        acl_type: 'Target Type',
        acl_topic: 'Topic',
        acl_access: 'Access Type',
        acl_authority: 'Authority',
        acl_item: 'Access Item',
        acl_connect_user: 'Connect User',
        acl_ip_addr: 'IP Address',
        acl_client_id: 'Client ID',
        acl_all: 'All',
        acl_sub: 'Sub',
        acl_pub: 'Pub',
        acl_subpub: 'Sub & Pub',
        acl_deny: 'Deny',
        acl_allow: 'Allow',
        acl_delete_target: 'Target: ',
        acl_delete_type: 'Target Type: ',
        acl_delete_topic: 'Topic: ',
        acl_delete_access: 'Access Type: ',
        acl_delete_authority: 'Authority: ',
        acl_add_title: 'Add Access Item',
        acl_edit_title: 'Edit Access Item',
        acl_delete_title: 'Delete Access Item',
        acl_delete_conf_text: 'Whether you want to delete the access item?',
        acl_add_succeeded: 'Add access item succeeded.',
        acl_add_failed: 'Add access item failed.',
        acl_edit_succeeded: 'Edit access item succeeded.',
        acl_edit_failed: 'Edit access item failed.',
        acl_delete_succeeded: 'Delete access item succeeded.',
        acl_delete_failed: 'Delete access item failed.',

        listener_protocol: 'Protocol',
        listener_address_port: 'Listen On',
        listener_conn_max_limit: 'Max Connections',
        listener_conn_count: 'Current Connections',

        ws_conn_title: 'Connect',
        ws_conn_path: 'Path',
        ws_conn_ssl: 'SSL',
        ws_conn_status: 'Connect Status: ',
        ws_conn_status_connected: 'Connected',
        ws_conn_status_disconnected: 'Disconnected',
        ws_sub_title: 'Subscribe',
        ws_sub_list: 'Subscribes',
        ws_sub_time: 'Time',
        ws_msg_title: 'Messages',
        ws_msg_retained: 'Retained',
        ws_msg_pub_list: 'Published Messages',
        ws_msg_sub_list: 'Received Messages',
        ws_msg_time: 'Time',
        ws_connect_btn: 'Connect',
        ws_disconnect_btn: 'Disconnect',
        ws_subscribe_btn: 'Subscribe',
        ws_send_btn: 'Send',
        ws_notify_connect_succeed: 'Connect succeed.',
        ws_notify_sub_succeed: 'Subscribe succeed.',
        ws_notify_unsub_succeed: 'Unsubscribe succeed.',
        ws_notify_send_succeed: 'Send succeed.',

        admin_account: 'Account',
        admin_desc: 'Comment',
        admin_passwd: 'Password',
        admin_passwd_conf: 'Confirm Password',
        admin_passwd_change: 'Change Password',
        admin_passwd_old: 'Old Password',
        admin_passwd_new: 'New Password',
        admin_passwd_new_conf: 'Confirm New Password',
        admin_add_title: 'Add Admin',
        admin_edit_title: 'Edit Admin',
        admin_delete_title: 'Delete Admin',
        admin_existed_message: 'Account is existed',
        admin_delete_conf_text: 'Whether you want to delete the adminstrator: ',
        admin_add_succeeded: 'Add administrator succeeded.',
        admin_add_failed: 'Add administrator failed.',
        admin_edit_succeeded: 'Edit administrator succeeded.',
        admin_edit_failed: 'Edit administrator failed.',
        admin_delete_succeeded: 'Delete administrator succeeded.',
        admin_delete_failed: 'Delete administrator failed.'
    };

    $.extend(locale_messages, messages_en_us);
})();