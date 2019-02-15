/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

export default class IgniteUiAceGeneratorFactory {
    constructor($scope, $attrs, Version, generatorFactory) {
        this.scope = $scope;
        this.attrs = $attrs;
        this.Version = Version;
        this.generatorFactory = generatorFactory;
    }

    $onInit() {
        delete this.data;

        const available = this.Version.available.bind(this.Version);

        // Setup generator.
        switch (this.generator) {
            case 'igniteConfiguration':
                this.generate = (cluster) => this.generatorFactory.cluster(cluster, this.Version.currentSbj.getValue(), this.client === 'true');

                break;
            case 'cacheStore':
            case 'cacheQuery':
                this.generate = (cache, domains) => {
                    const cacheDomains = _.reduce(domains, (acc, domain) => {
                        if (_.includes(cache.domains, domain.value))
                            acc.push(domain.meta);

                        return acc;
                    }, []);

                    return this.generatorFactory[this.generator](cache, cacheDomains, available);
                };

                break;
            case 'cacheNodeFilter':
                this.generate = (cache, igfss) => {
                    const cacheIgfss = _.reduce(igfss, (acc, igfs) => {
                        acc.push(igfs.igfs);

                        return acc;
                    }, []);

                    return this.generatorFactory.cacheNodeFilter(cache, cacheIgfss);
                };

                break;
            case 'clusterServiceConfiguration':
                this.generate = (cluster, caches) => {
                    return this.generatorFactory.clusterServiceConfiguration(cluster.serviceConfigurations, caches);
                };

                break;
            case 'clusterCheckpoint':
                this.generate = (cluster, caches) => {
                    return this.generatorFactory.clusterCheckpoint(cluster, available, caches);
                };

                break;
            case 'igfss':
                this.generate = (cluster, igfss) => {
                    const clusterIgfss = _.reduce(igfss, (acc, igfs) => {
                        if (_.includes(cluster.igfss, igfs.value))
                            acc.push(igfs.igfs);

                        return acc;
                    }, []);

                    return this.generatorFactory.clusterIgfss(clusterIgfss, available);
                };

                break;
            default:
                this.generate = (master) => this.generatorFactory[this.generator](master, available);
        }
    }

    $postLink() {
        if (this.formCtrl && this.ngModelCtrl)
            this.formCtrl.$removeControl(this.ngModelCtrl);

        if (this.igniteUiAceTabs && this.igniteUiAceTabs.onLoad) {
            this.scope.onLoad = (editor) => {
                this.igniteUiAceTabs.onLoad(editor);

                this.scope.$watch('master', () => editor.attractAttention = false);
            };
        }

        if (this.igniteUiAceTabs && this.igniteUiAceTabs.onChange)
            this.scope.onChange = this.igniteUiAceTabs.onChange;

        const noDeepWatch = !(typeof this.attrs.noDeepWatch !== 'undefined');

        const next = () => {
            this.ctrl.data = _.isNil(this.scope.master) ? null : this.ctrl.generate(this.scope.master, this.scope.detail).asString();
        };

        // Setup watchers.
        this.scope.$watch('master', next, noDeepWatch);

        this.subscription = this.Version.currentSbj.subscribe({next});
    }

    $onDestroy() {
        this.subscription.unsubscribe();
    }
}
