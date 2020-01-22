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

type InputModes = 'text' | 'number' | 'email' | 'date' | 'time' | 'date-and-time';

interface ValidationFunction<T> {
    (value: T): boolean
}

/**
 * Options for rendering inputs.
 */
interface InputOptions<T> {
    /** Input type. */
    mode?: InputModes,
    /** Dialog title. */
    title?: string,
    /** Input field label. */
    label?: string,
    /** Message for tooltip in label. */
    tip?: string,
    /** Default value. */
    value: T,
    /** Placeholder for input. */
    placeholder?: string,
    /** Validator function. */
    toValidValue?: ValidationFunction<T>,
    /** Min value for number input. */
    min?: number,
    /** Max value for number input. */
    max?: number,
    /** Postfix for units in number input. */
    postfix?: string
}

export default class InputDialog {
    static $inject = ['$modal', '$q'];

    constructor(private $modal: mgcrea.ngStrap.modal.IModalService, private $q: ng.IQService) {}

    /**
     * Fabric for creating modal instance with different input types.
     *
     * @returns User input.
     */
    private dialogFabric<T>(args: InputOptions<T>) {
        const deferred = this.$q.defer<T>();

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
     * @param title Dialog title.
     * @param label Input field label.
     * @param value Default value.
     * @param toValidValue Validator function.
     * @param mode Input type.
     */
    input<T>(title: string, label: string, value: T, toValidValue?: ValidationFunction<T>, mode: InputModes = 'text') {
        return this.dialogFabric<T>({title, label, value, toValidValue, mode});
    }

    /**
     * Open input dialog to configure cloned object name.
     *
     * @param srcName Name of source object.
     * @param names List of already exist names.
     * @returns New name.
     */
    clone(srcName: string, names: Array<string>) {
        const uniqueName = (value) => {
            let num = 1;
            let tmpName = value;

            while (_.includes(names, tmpName)) {
                tmpName = `${value}_${num}`;

                num++;
            }

            return tmpName;
        };

        return this.input<string>('Clone', 'New name', uniqueName(srcName), uniqueName);
    }

    /**
     * Open input dialog to configure custom number value.
     *
     * @param options Object with settings for rendering number input.
     * @returns User input.
     */
    number(options: InputOptions<number>) {
        return this.dialogFabric({mode: 'number', ...options});
    }

    /**
     * Open input dialog to configure custom e-mail.
     *
     * @param options Object with settings for rendering e-mail input.
     * @return User input.
     */
    email(options: InputOptions<string>) {
        return this.dialogFabric({mode: 'email', ...options});
    }

    /**
     * Open input dialog to configure custom date value.
     *
     * @param options Settings for rendering date input.
     * @returns User input.
     */
    date(options: InputOptions<Date>) {
        return this.dialogFabric({mode: 'date', ...options});
    }

    /**
     * Open input dialog to configure custom time value.
     *
     * @param options Settings for rendering time input.
     * @returns User input.
     */
    time(options: InputOptions<Date>) {
        return this.dialogFabric({mode: 'time', ...options});
    }

    /**
     * Open input dialog to configure custom date and time value.
     *
     * @param options Settings for rendering date and time inputs.
     * @returns User input.
     */
    dateTime(options: InputOptions<Date>) {
        return this.dialogFabric({mode: 'date-and-time', ...options});
    }
}
