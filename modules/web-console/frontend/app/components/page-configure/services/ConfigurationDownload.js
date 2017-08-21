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

import saver from 'file-saver';

export default class ConfigurationDownload {
    static $inject = [
        'IgniteMessages',
        'IgniteActivitiesData',
        'IgniteConfigurationResource',
        'IgniteSummaryZipper',
        'IgniteVersion',
        '$q',
        '$rootScope'
    ];

    constructor(messages, activitiesData, configuration, summaryZipper, Version, $q, $rootScope) {
        Object.assign(this, {messages, activitiesData, configuration, summaryZipper, Version, $q, $rootScope});

        this.saver = saver;
    }

    downloadClusterConfiguration(cluster) {
        this.activitiesData.post({action: '/configuration/download'});

        return this.configuration.read()
            .then((data) => this.configuration.populate(data))
            .then(({clusters}) => {
                return clusters.find(({_id}) => _id === cluster._id)
                    || this.$q.reject({message: `Cluster ${cluster.name} not found`});
            })
            .then((cluster) => {
                return this.summaryZipper({
                    cluster,
                    data: {},
                    IgniteDemoMode: this.$rootScope.IgniteDemoMode,
                    targetVer: this.Version.currentSbj.getValue()
                });
            })
            .then((data) => {
                const fileName = `${this.escapeFileName(cluster.name)}-project.zip`;

                this.saver.saveAs(data, fileName);
            })
            .catch((e) => (
                this.messages.showError(`Failed to generate project files. ${e.message}`)
            ));
    }

    escapeFileName(name) {
        return name.replace(/[\\\/*\"\[\],\.:;|=<>?]/g, '-').replace(/ /g, '_');
    }
}
