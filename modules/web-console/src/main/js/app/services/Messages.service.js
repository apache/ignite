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

// Service to show various information and error messages.
export default ['IgniteMessages', ['$alert', ($alert) => {
    // Common instance of alert modal.
    let msgModal;

    function _showMessage(msg, type, duration, icon) {
        if (msgModal)
            msgModal.hide();

        const title = msg ? msg.toString() : 'Internal error.';

        msgModal = $alert({type, title, duration});

        msgModal.$scope.icon = icon;
    }

    return {
        errorMessage(prefix, err) {
            return prefix + (err ? err.toString() : 'Internal error.');
        },
        showError(msg) {
            _showMessage(msg, 'danger', 10, 'fa-exclamation-triangle');

            return false;
        },
        showInfo(msg) {
            _showMessage(msg, 'success', 3, 'fa-check-circle-o');
        },
        hideAlert() {
            if (msgModal)
                msgModal.hide();
        }
    };
}]];
