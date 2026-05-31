

export default class {
    static $inject = ['clusters'];

    constructor(clusters) {
        this.clusters = _.map(clusters, (cluster) =>
            _.merge({}, cluster, { size: _.size(cluster?cluster.nodes:[]) })
        );
    }
}
