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

var thisNodeId = null;

$(function() {
    $.ajaxSetup({
        beforeSend: function(xhr) {
            if (thisNodeId) {
                xhr.setRequestHeader('paramNodeId', thisNodeId);
            }
        }
    });

    if (window.history && window.history.pushState) {
        $(window).on('popstate', function() {
            // get location hash
            var hash = window.location.hash;

            if (hash != '') {
                loadTemplate(hash.substr(1));
            } else {
                window.history.go(-1);
            }

            return false;
        });
    }

    $.ajax({
        url: '/sys/index/init',
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({}),
        success: function(res) {
            if (res && res.code === 1 && res.result) {
                thisNodeId = res.result.nodeId;

                initIndexMenu(res.result.localAuth);

                initIndexNodeList(res.result.nodes, res.result.nodeId);
            }
        }
    });

    // bind logout click event
    $('#logout-link').click(function() {
        window.location.href = '/logout';
    });

    // get language
    var lang = getLanguage();

    loadLanguage(lang, function() {
        // initialize node list
        $('#system-node-list').select2({
            language: lang
        });

        // initialize content wrapper html
        initIndex();
    });
});

function initIndexMenu(islocalAuth) {
    if (islocalAuth) {
        $('#user-treeview').show();
        $('#acl-treeview').show();
    } else {
        $('#user-treeview').hide();
        $('#acl-treeview').hide();
    }
}

function initIndexNodeList(nodes, value) {
    $('#system-node-list').off('select2:select');

    $('#system-node-list').select2({
        data: nodes
    });

    $('#system-node-list').on('select2:select', function(e) {
        thisNodeId = $(this).val();

        $.ajax({
            url: '/sys/index/init',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({}),
            success: function(res) {
                if (res && res.code === 1 && res.result) {
                    thisNodeId = res.result.nodeId;

                    initIndexMenu(res.result.localAuth);

                    initIndexNodeList(res.result.nodes, res.result.nodeId);

                    initIndex();
                }
            }
        });
    });

    $('#system-node-list').val(value);
}

function initIndex() {
    // get location hash
    var hash = window.location.hash;

    if (hash == '') {
        window.location.hash = 'console';
        hash = 'console';
    } else {
        hash = hash.substr(1);
    }

    loadTemplate(hash);
}

function loadTemplate(hash) {
    // remove class
    $('.sidebar-menu').find('.treeview').removeClass('menu-open');

    // add class
    $('a[id=' + hash + ']').closest('.treeview').addClass('menu-open');

    // load html
    $('.content-wrapper').load(hash + '.html', function() {
        // remove modal open class
        $('body').removeClass('modal-open');

        // remove modal backdrop
        $('.modal-backdrop').remove();

        // change language
        changeLanguage();
    });
}