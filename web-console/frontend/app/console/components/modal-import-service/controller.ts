
import {default as Clusters} from 'app/configuration/services/Clusters';
import {Cluster,ShortCluster} from 'app/configuration/types';
import {default as ConfigureState} from 'app/configuration/services/ConfigureState';
import IgniteVersion from 'app/services/Version.service';
import IgniteLoadingFactory from 'app/modules/loading/loading.service';
import FormUtils from 'app/services/FormUtils.service';
import MessagesFactory from 'app/services/Messages.service';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {Menu} from 'app/types';

import {
    changeItem,
    removeClusterItems,
    basicSave,
    advancedSaveCluster
} from 'app/configuration/store/actionCreators';

export default class ModalImportServiceController {
    static $inject = [    
        'IgniteVersion',
        '$scope',
        'ConfigureState',
        'Clusters',
        'AgentManager',
        'IgniteFormUtils',
        'IgniteLoading',
        'IgniteMessages'
    ];

    constructor(       
        private IgniteVersion: IgniteVersion,
        private $scope: ng.IScope,
        private ConfigureState: ConfigureState,
        private Clusters: Clusters,
        private AgentManager: AgentManager, 
        private IgniteFormUtils: ReturnType<typeof FormUtils>,
        private IgniteLoading: ReturnType<typeof IgniteLoadingFactory>,
        private IgniteMessages: ReturnType<typeof MessagesFactory>
    ) {}

    form: ng.IFormController;
    onHide: ng.ICompiledExpression;
    cluster: Cluster;
    isDemo: boolean;

    cachesMenu: Menu<string>;
    servicesCachesMenu: Menu<string>;
    serviceConfiguration: any;

    $onInit() {
        this.serviceConfiguration = this.Clusters.makeBlankServiceConfiguration();
        this.Clusters.getClusterCaches(this.cluster.id).then((caches)=>{
            this.cachesMenu = caches.data.map((c) => ({label: c.name, value: c.id}));
            this.servicesCachesMenu = [{label: 'Key-affinity not used', value: null}].concat(this.cachesMenu);
        });
    }
   
    addServiceConfiguration() {
        if (this.form.$invalid)
            return this.IgniteFormUtils.triggerValidation(this.form, this.$scope);
        
        for(let i = 0; i < this.cluster.serviceConfigurations.length; i++){
            if(this.cluster.serviceConfigurations[i].service == this.serviceConfiguration.service){
                this.IgniteMessages.showError('Service configuration with the same serviceClass already exists');
                return;
            }
        }
        this.AgentManager.callClusterService(this.cluster,"deployService",{'service':this.serviceConfiguration}).then((msg)=>{
            if(msg.status == 'success'){
                this.IgniteMessages.showInfo('Service configuration has been added');
                this.cluster.serviceConfigurations.push(this.serviceConfiguration);
                this.ConfigureState.dispatchAction(advancedSaveCluster(this.cluster));
                this.onHide();
            }else{
                this.IgniteMessages.showError('Error while adding service configuration:'+msg.message);
            }
            
        })
        
    }
}
