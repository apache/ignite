/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

        this.showIgniteLogo = false;

        this.footerHtml = [
            `<p></p>`,
            '<p></p>'            
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
