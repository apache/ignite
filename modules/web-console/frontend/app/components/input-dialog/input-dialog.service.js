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

import _ from 'lodash';
import controller from './input-dialog.controller';
import templateUrl from './input-dialog.tpl.pug';
import {CancellationError} from 'app/errors/CancellationError';

export default class InputDialog {
    static $inject = ['$modal', '$q'];

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IQService} $q
     */
    constructor($modal, $q) {
        this.$modal = $modal;
        this.$q = $q;
    }

    /**
     * Fabric for creating modal instance with different input types.
     *
     * @param {Object} args Options for rendering inputs:
     * @param {'text'|'number'} args.mode Input type.
     * @param {String} args.title Dialog title.
     * @param {String} args.label Input field label.
     * @param {String} args.tip Message for tooltip in label.
     * @param {String|Number} args.value Default value.
     * @param {String} args.placeholder Placeholder for input.
     * @param {Function} [args.toValidValue] Validator function.
     * @param {Number} args.min Min value for number input.
     * @param {Number} args.max Max value for number input.
     * @param {String} args.postfix Postfix for units in numer input.
     * @return {Promise.<String>} User input.
     */
    dialogFabric(args) {
        const deferred = this.$q.defer();

        const modal = this.$modal({
            templateUrl,
            resolve: {
                deferred: () => deferred,
                ui: () => args
            },
            controller,
            controllerAs: 'ctrl'
        });

        const modalHide = modal.hide;

        modal.hide = () => deferred.reject(new CancellationError());

        return deferred.promise
            .finally(modalHide);
    }

    /**
     * Open input dialog to configure custom value.
     *
     * @param {String} title Dialog title.
     * @param {String} label Input field label.
     * @param {String} value Default value.
     * @param {Function} [toValidValue] Validator function.
     * @param {'text'|'number'} mode Input type.
     * @returns {ng.IPromise<string>} User input.
     */
    input(title, label, value, toValidValue, mode = 'text') {
        return this.dialogFabric({title, label, value, toValidValue, mode});
    }

    /**
     * Open input dialog to configure cloned object name.
     *
     * @param {String} srcName Name of source object.
     * @param {Array.<String>} names List of already exist names.
     * @returns {ng.IPromise<string>} New name.
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

    /**
     * Open input dialog to configure custom number value.
     *
     * @param {Object} options Object with settings for rendering number input.
     * @returns {Promise.<String>} User input.
     */
    number(options) {
        return this.dialogFabric({mode: 'number', ...options});
    }
}
