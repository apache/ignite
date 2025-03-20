

import _ from 'lodash';

export default function ConfigurationResourceService() {
    return {
        populate(data) {
            const {spaces, clusters, caches, domains} = _.cloneDeep(data);

            _.forEach(clusters, (cluster) => {
                cluster.caches = _.filter(caches, ({id}) => _.includes(cluster.caches, id));

                _.forEach(cluster.caches, (cache) => {
                    cache.domains = _.filter(domains, ({id}) => _.includes(cache.domains, id));
                });
            });

            return Promise.resolve({spaces, clusters, caches, domains});
        }
    };
}
