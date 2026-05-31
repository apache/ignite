

import saver from 'file-saver';
import {ClusterLike} from '../types';
import MessagesFactory from 'app/services/Messages.service';
import Activities from 'app/core/activities/Activities.data';
import ConfigurationResource from './ConfigurationResource';
import SummaryZipper from './SummaryZipper';
import Version from 'app/services/Version.service';
import PageConfigure from './PageConfigure';
import {DemoService} from 'app/modules/demo/Demo.module';
import {Base64}  from 'js-base64';

export default class ConfigurationDownload {
    static $inject = [
        'IgniteMessages',
        'IgniteActivitiesData',
        'IgniteConfigurationResource',
        'IgniteSummaryZipper',
        'IgniteVersion',
        '$q',
        'Demo',
        'PageConfigure'        
    ];

    constructor(
        private messages: ReturnType<typeof MessagesFactory>,
        private activitiesData: Activities,
        private configuration: ConfigurationResource,
        private summaryZipper: SummaryZipper,
        private Version: Version,
        private $q: ng.IQService,
        private Demo: DemoService,
        private PageConfigure: PageConfigure        
    ) {}

    saver = saver;

    downloadClusterConfiguration(cluster: ClusterLike) {
        this.activitiesData.post({action: '/configuration/download'});

        return this.PageConfigure.getClusterConfiguration({clusterID: cluster.id, isDemo: !!this.Demo.enabled})
            .then((data) => this.configuration.populate(data))
            .then(({clusters}) => {
                return clusters.find(({id}) => id === cluster.id)
                    || this.$q.reject({message: `Cluster ${cluster.name} not found`});
            })
            .then((cluster) => {
                return this.summaryZipper({
                    cluster,
                    data: {},
                    demoMode: this.Demo.enabled,
                    targetVer: this.Version.currentSbj.getValue()
                });
            })
            .then((data) => this.saver.saveAs(data, this.nameFile(cluster)))
            .catch((e) => (
                this.messages.showError(`Failed to generate project files. ${e.message}`)
            ));
    }

    nameFile(cluster: ClusterLike) {
        return `${this.escapeFileName(cluster.name)}-project.zip`;
    }

    escapeFileName(name: string) {
        return name.replace(/[\\\/*\"\[\],\.:;|=<>?]/g, '-').replace(/ /g, '_');
    }
    
    generateDownloadData(cluster: ClusterLike){
        return this.PageConfigure.getClusterConfiguration({clusterID: cluster.id, isDemo: !!this.Demo.enabled})
            .then((data) => this.configuration.populate(data))
            .then(({clusters}) => {
                return clusters.find(({id}) => id === cluster.id)
                    || this.$q.reject({message: `Cluster ${cluster.name} not found`});
            })
            .then((cluster) => {
                return this.summaryZipper({
                    cluster,
                    data: {},
                    demoMode: this.Demo.enabled,
                    targetVer: this.Version.currentSbj.getValue()
                });
            })
            .then(this.blobToBase64);
    }
    
    blobToBase64(blob:Blob) {
      return new Promise((resolve, reject) => {
        const fileReader = new FileReader();
        fileReader.onload = (e) => {
          resolve(e.target.result);
        };
        // readAsDataURL
        fileReader.readAsDataURL(blob);
        fileReader.onerror = () => {
          reject(new Error('blobToBase64 error'));
        };
      });
    }
}
