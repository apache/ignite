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

export default ['IgniteBranding', [function() {
    let titleSuffix = ' – Apache Ignite Web Console';

    let headerLogo = '/images/ignite-logo.png';

    let headerText = 'Management console for Apache Ignite';

    let showIgniteLogo = false;

    let footerHtml = [
        '<p>Apache Ignite Web Console</p>',
        '<p>© 2017 The Apache Software Foundation.</p>',
        '<p>Apache, Apache Ignite, the Apache feather and the Apache Ignite logo are trademarks of The Apache Software Foundation.</p>'
    ];

    let termsState;

    let featuresHtml = [
        '<p>Web Console is an interactive management tool which allows to:</p>',
        '<ul>',
        '   <li>Create and download cluster configurations</li>',
        '   <li>Automatically import domain model from any RDBMS</li>',
        '   <li>Connect to cluster and run SQL analytics on it</li>',
        '</ul>'
    ];

    /**
     * Change title suffix.
     *
     * @param {String} suffix.
     */
    this.titleSuffix = (suffix) => {
        titleSuffix = suffix;
    };

    /**
     * Change logo in header.
     *
     * @param {String} url Logo path.
     */
    this.headerLogo = (url) => {
        headerLogo = url;

        showIgniteLogo = true;
    };

    /**
     * Change text in header.
     *
     * @param {String} text Header text.
     */
    this.headerText = (text) => {
        headerText = text;
    };

    /**
     * Change text in features.
     *
     * @param {Array.<String>} rows Features text.
     */
    this.featuresHtml = (rows) => {
        featuresHtml = rows;
    };

    /**
     * Change text in footer.
     *
     * @param {Array.<String>} rows Footer text.
     */
    this.footerHtml = (rows) => {
        footerHtml = rows;
    };

    /**
     * Set terms and conditions stage.
     *
     * @param {String} state
     */
    this.termsState = (state) => {
        termsState = state;
    };

    this.$get = [() => {
        return {
            titleSuffix,
            headerLogo,
            headerText,
            featuresHtml: featuresHtml.join('\n'),
            footerHtml: footerHtml.join('\n'),
            showIgniteLogo,
            termsState
        };
    }];
}]];
