import _ from 'lodash';

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
                    return this.generatorFactory.cacheNodeFilter(cache, igfss);
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
