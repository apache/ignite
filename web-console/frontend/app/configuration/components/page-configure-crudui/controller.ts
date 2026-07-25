

export default class PageConfigureCrudUIController {
    static menuItems = [
        { text: 'Cluster', sref: 'base.configuration.edit.crudui.cluster' },
        { text: 'SQL Scheme', sref: 'base.configuration.edit.crudui.models' },
        { text: 'Caches', sref: 'base.configuration.edit.crudui.caches' },
    ];

    menuItems: Array<{text: string, sref: string}>;

    $onInit() {
        this.menuItems = PageConfigureCrudUIController.menuItems;
    }
}
