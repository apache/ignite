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

$.validator.addMethod("swtichCondRequired", function(value, element, params) {
    if ($(params).bootstrapSwitch('state')) {
        return value.length > 0;
    }

    return true;
});

$.validator.addMethod("notEqualTo", function(value, element, params) {
    if (!$(element).is(':visible')) {
        return true;
    }

    var targetValue = $(params).val();

    if (value === targetValue) {
        return false;
    }

    return true;
});

function errorPlacement4Valid(error, element) {
    var $group = $(element).closest(".form-group");
    var $help = $group.find(".help-block");

    $help.html(error.text());
    $help.show();

    $group.addClass("has-error");
}

function handleError4Valid(message, selector) {
    var $group = $(selector).closest(".form-group");
    var $help = $group.find(".help-block");

    $help.html(message);
    $help.show();

    $group.addClass("has-error");
}

function success4Valid(label, element) {
    var $group = $(element).closest(".form-group");
    var $help = $group.find(".help-block");

    $help.html('');
    $help.hide();

    $group.removeClass("has-error");
}

function addAdminRules() {
    return {
        'add-admin-account': {
            required: true,
            maxlength: 30
        },
        'add-admin-desc': {
            maxlength: 100
        },
        'add-admin-passwd': {
            required: true,
            rangelength: [6, 16]
        },
        'add-admin-passwd-conf': {
            required: true,
            rangelength: [6, 16],
            equalTo: '#add-admin-passwd'
        }
    };
}

function addAdminMessages() {
    return {
        'add-admin-passwd-conf': {
            equalTo: locale_messages.common_passwd_equal
        }
    };
}

function editAdminRules() {
    return {
        'edit-admin-desc': {
            maxlength: 100
        },
        'edit-admin-old-passwd': {
            swtichCondRequired: '#edit-admin-passwd-update',
            rangelength: [6, 16]
        },
        'edit-admin-passwd': {
            swtichCondRequired: '#edit-admin-passwd-update',
            rangelength: [6, 16],
            notEqualTo: '#edit-admin-old-passwd'
        },
        'edit-admin-passwd-conf': {
            swtichCondRequired: '#edit-admin-passwd-update',
            rangelength: [6, 16],
            equalTo: '#edit-admin-passwd'
        }
    };
}

function editAdminMessages() {
    return {
        'edit-admin-old-passwd': {
            swtichCondRequired: locale_messages.common_swtich_cond_required
        },
        'edit-admin-passwd': {
            swtichCondRequired: locale_messages.common_swtich_cond_required,
            notEqualTo: locale_messages.common_passwd_not_equal
        },
        'edit-admin-passwd-conf': {
            swtichCondRequired: locale_messages.common_swtich_cond_required,
            equalTo: locale_messages.common_passwd_equal
        }
    };
}

function addUserRules() {
    return {
        'add-conn-username': {
            required: true,
            maxlength: 30
        },
        'add-user-desc': {
            maxlength: 100
        },
        'add-user-passwd': {
            required: true,
            rangelength: [6, 16]
        },
        'add-user-passwd-conf': {
            required: true,
            rangelength: [6, 16],
            equalTo: '#add-user-passwd'
        }
    };
}

function addUserMessages() {
    return {
        'add-user-passwd-conf': {
            equalTo: locale_messages.common_passwd_equal
        }
    };
}

function editUserRules() {
    return {
        'edit-user-desc': {
            maxlength: 100
        },
        'edit-user-admin-passwd': {
            swtichCondRequired: '#edit-user-passwd-update',
            rangelength: [6, 16]
        },
        'edit-user-passwd': {
            swtichCondRequired: '#edit-user-passwd-update',
            rangelength: [6, 16]
        },
        'edit-user-passwd-conf': {
            swtichCondRequired: '#edit-user-passwd-update',
            rangelength: [6, 16],
            equalTo: '#edit-user-passwd'
        }
    };
}

function editUserMessages() {
    return {
        'edit-user-admin-passwd': {
            swtichCondRequired: locale_messages.common_swtich_cond_required
        },
        'edit-user-passwd': {
            swtichCondRequired: locale_messages.common_swtich_cond_required
        },
        'edit-user-passwd-conf': {
            swtichCondRequired: locale_messages.common_swtich_cond_required,
            equalTo: locale_messages.common_passwd_equal
        }
    };
}

function aclRules() {
    return {
        'acl-target': {
            required: true,
            maxlength: 1024
        },
        'acl-topic': {
            required: true,
            maxlength: 1024
        }
    };
}