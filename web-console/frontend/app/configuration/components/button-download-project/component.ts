

import template from './template.pug';
import ConfigurationDownload from '../../services/ConfigurationDownload';

export class ButtonDownloadProject {
    static $inject = ['ConfigurationDownload'];

    constructor(private ConfigurationDownload: ConfigurationDownload) {}

    cluster: any;

    download() {
        return this.ConfigurationDownload.downloadClusterConfiguration(this.cluster);
    }
}
export const component = {
    name: 'buttonDownloadProject',
    controller: ButtonDownloadProject,
    template,
    bindings: {
        cluster: '<'
    }
};
