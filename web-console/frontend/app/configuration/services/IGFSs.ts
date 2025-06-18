
import _ from 'lodash';
import uuidv4 from 'uuid/v4';
import omit from 'lodash/fp/omit';
import get from 'lodash/get';

export default class IGFSs {
    static $inject = ['$http'];

    igfsModes = [
        {value: 'PRIMARY', label: 'PRIMARY'},
        {value: 'PROXY', label: 'PROXY'},
        {value: 'DUAL_SYNC', label: 'DUAL_SYNC'},
        {value: 'DUAL_ASYNC', label: 'DUAL_ASYNC'}
    ];

    constructor(private $http: ng.IHttpService) {}

    getIGFS(igfsID: string) {
        return this.$http.get(`/api/v1/configuration/igfss/${igfsID}`);
    }

    getBlankIGFS() {
        return {
            id: uuidv4(),
            ipcEndpointEnabled: true,
            fragmentizerEnabled: true,
            colocateMetadata: true,
            relaxedConsistency: true,
            secondaryFileSystem: {
                kind: 'Caching'
            }
        };
    }

    backups = {
        default: 0,
        min: 0
    };

    defaultMode = {
        values: [
            {value: 'PRIMARY', label: 'PRIMARY'},
            {value: 'PROXY', label: 'PROXY'},
            {value: 'DUAL_SYNC', label: 'DUAL_SYNC'},
            {value: 'DUAL_ASYNC', label: 'DUAL_ASYNC'}
        ],
        default: 'DUAL_ASYNC'
    };

    secondaryFileSystemEnabled = {
        requiredWhenIGFSProxyMode: (igfs) => {
            if (get(igfs, 'defaultMode') === 'PROXY')
                return get(igfs, 'secondaryFileSystemEnabled') === true;

            return true;
        },
        requiredWhenPathModeProxyMode: (igfs) => {
            if (get(igfs, 'pathModes', []).some((pm) => pm.mode === 'PROXY'))
                return get(igfs, 'secondaryFileSystemEnabled') === true;

            return true;
        }
    };

    normalize = omit(['__v', 'space', 'clusters']);

    addSecondaryFsNameMapper(igfs) {
        if (!_.get(igfs, 'secondaryFileSystem.userNameMapper.Chained.mappers'))
            _.set(igfs, 'secondaryFileSystem.userNameMapper.Chained.mappers', []);

        const item = {id: uuidv4(), kind: 'Basic'};

        _.get(igfs, 'secondaryFileSystem.userNameMapper.Chained.mappers').push(item);

        return item;
    }
}
