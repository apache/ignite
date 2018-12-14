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

import angular from 'angular';

/**
 * Service to copy some value to OS clipboard.
 * @param {ng.IWindowService} $window
 * @param {ReturnType<typeof import('./Messages.service').default>} Messages
 */
export default function factory($window, Messages) {
    const body = angular.element($window.document.body);

    /** @type {JQuery<HTMLTextAreaElement>} */
    const textArea = angular.element('<textarea/>');

    textArea.css({
        position: 'fixed',
        opacity: '0'
    });

    return {
        /**
         * @param {string} toCopy
         */
        copy(toCopy) {
            textArea.val(toCopy);

            body.append(textArea);

            textArea[0].select();

            try {
                if (document.execCommand('copy'))
                    Messages.showInfo('Value copied to clipboard');
                else
                    window.prompt('Copy to clipboard: Ctrl+C, Enter', toCopy); // eslint-disable-line no-alert
            }
            catch (err) {
                window.prompt('Copy to clipboard: Ctrl+C, Enter', toCopy); // eslint-disable-line no-alert
            }

            textArea.remove();
        }
    };
}

factory.$inject = ['$window', 'IgniteMessages'];
