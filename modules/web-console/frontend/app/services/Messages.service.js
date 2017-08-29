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

    const errorMessage = (prefix, err) => {
        prefix = prefix || '';

        if (err) {
            if (err.hasOwnProperty('data'))
                err = err.data;

            if (err.hasOwnProperty('message'))
                return prefix + err.message;

            return prefix + err;
        }

        return prefix + 'Internal error.';
    };

    const hideAlert = () => {
        if (msgModal)
            msgModal.hide();
    };

    const _showMessage = (message, err, type, duration) => {
        hideAlert();

        const title = err ? errorMessage(message, err) : errorMessage(null, message);

        msgModal = $alert({type, title, duration});

        msgModal.$scope.icon = `icon-${type}`;
    };

    return {
        errorMessage,
        hideAlert,
        showError(message, err) {
            _showMessage(message, err, 'danger', 10);

            return false;
        },
        showInfo(message) {
            _showMessage(message, null, 'success', 3);
        }
    };
}]];
