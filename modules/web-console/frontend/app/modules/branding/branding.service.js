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

export default class {
    static $inject = ['IgniteVersion'];

    /**
     * @param {import('app/services/Version.service').default} Version
     */
    constructor(Version) {
        this.titleSuffix = ' - Apache Ignite Web Console';

        this.headerLogo = '/images/ignite-logo.svg';

        this.headerText = 'Management console for Apache Ignite';

        this.showIgniteLogo = false;

        this.footerHtml = [
            `<p>Apache Ignite Web Console (${Version.webConsole})</p>`,
            '<p>© 2018 The Apache Software Foundation.</p>',
            '<p>Apache, Apache Ignite, the Apache feather and the Apache Ignite logo are trademarks of The Apache Software Foundation.</p>'
        ].join('\n');

        this.termsState = null;

        this.featuresHtml = [
            '<p>Web Console is an interactive management tool which allows to:</p>',
            '<ul>',
            '   <li>Create and download cluster configurations</li>',
            '   <li>Automatically import domain model from any RDBMS</li>',
            '   <li>Connect to cluster and run SQL analytics on it</li>',
            '</ul>'
        ].join('\n');
    }
}
