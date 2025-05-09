import template from './template.pug';
import './style.scss';
import ModalImportService from '../modal-import-service/service';
import {Cluster} from 'app/configuration/types';

export class ButtonImportService {
    static $inject = ['ModalImportService','$location','$scope'];

    constructor(private ModalImportService: ModalImportService,private $location: ng.ILocationService,private $scope: ng.IScope) {
        this.$scope.toolButtonVisible = true;
    }

    cluster: Cluster;

    $onInit() {
        const path = this.$location.path();
        if(path.indexOf('service')>=0){
            this.$scope.toolButtonVisible = true;
        }        
    }

    openDeployServiceModal() {
        return this.ModalImportService.open(this.cluster);
    }
}

export const component = {
    name: 'buttonImportService',
    controller: ButtonImportService,
    template,
    bindings: {
        cluster: '<cluster'    
    }
};