

import {ClusterSecrets} from './ClusterSecrets';

export class ClusterSecretsManager {
    /** @type {Map<String, ClusterSecrets>} */
    memoryCache = new Map();

    /**
     * @param {String} clusterId
     * @private
     */
    _has(clusterId) {
        return this.memoryCache.has(clusterId);
    }

    /**
     * @param {String} clusterId
     * @private
     */
    _get(clusterId) {
        return this.memoryCache.get(clusterId);
    }

    /**
     * @param {String} clusterId
     */
    get(clusterId) {
        if (this._has(clusterId))
            return this._get(clusterId);

        const secrets = new ClusterSecrets();

        this.put(clusterId, secrets);

        return secrets;
    }

    /**
     * @param {String} clusterId
     * @param {ClusterSecrets} secrets
     */
    put(clusterId, secrets) {
        this.memoryCache.set(clusterId, secrets);
    }

    /**
     * @param {String} clusterId
     */
    reset(clusterId) {
        const secrets = this._get(clusterId);

        secrets && secrets.resetCredentials();
    }
}
