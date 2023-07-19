

import get from 'lodash/get';
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';

import {CacheModes, AtomicityModes, ShortCache} from 'app/configuration/types';
import {Menu} from 'app/types';

export default class Services {
    static $inject = ['$http'];

    serviceModes: Menu<CacheModes> = [
        {value: 'NodeSingleton', label: 'NodeSingleton'},
        {value: 'ClusterSingleton', label: 'ClusterSingleton'},
        {value: 'KeyAffinitySingleton', label: 'KeyAffinitySingleton'},
        {value: 'Multiple', label: 'Multiple'}
    ];

    atomicityModes: Menu<AtomicityModes> = [
        {value: 'ATOMIC', label: 'ATOMIC'},
        {value: 'TRANSACTIONAL', label: 'TRANSACTIONAL'},
        {value: 'TRANSACTIONAL_SNAPSHOT', label: 'TRANSACTIONAL_SNAPSHOT'}
    ];

    constructor(private $http: ng.IHttpService) {}

    getBackupsCount(serviceName){
        return 1;
    }
}
