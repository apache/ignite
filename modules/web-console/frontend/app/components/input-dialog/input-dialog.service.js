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

import controller from './input-dialog.controller';
import templateUrl from './input-dialog.tpl.pug';

export default class InputDialog {
    static $inject = ['$modal', '$q'];

    constructor($modal, $q) {
        this.$modal = $modal;
        this.$q = $q;
    }

    /**
     * Open input dialog to configure custom value.
     *
     * @param {String} title Dialog title.
     * @param {String} label Input field label.
     * @param {String} value Default value.
     * @param {Function} [toValidValue] Validator function.
     * @returns {Promise.<String>} User input.
     */
    input(title, label, value, toValidValue) {
        const deferred = this.$q.defer();

        const modal = this.$modal({
            templateUrl,
            resolve: {
                deferred: () => deferred,
                ui: () => ({
                    title,
                    label,
                    value,
                    toValidValue
                })
            },
            controller,
            controllerAs: 'ctrl'
        });

        const modalHide = modal.hide;

        modal.hide = () => deferred.reject('cancelled');

        return deferred.promise
            .finally(modalHide);
    }

    /**
     * Open input dialog to configure cloned object name.
     *
     * @param {String} srcName Name of source object.
     * @param {Array.<String>} names List of already exist names.
     * @returns {Promise.<String>} New name
     */
    clone(srcName, names) {
        const uniqueName = (value) => {
            let num = 1;
            let tmpName = value;

            while (_.includes(names, tmpName)) {
                tmpName = `${value}_${num}`;

                num++;
            }

            return tmpName;
        };

        return this.input('Clone', 'New name', uniqueName(srcName), uniqueName);
    }
}
