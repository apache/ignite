

export default class PageConfigureAdvancedController {
    static menuItems = [
        { text: 'Cluster', sref: 'base.configuration.edit.advanced.cluster' },
        { text: 'SQL Scheme', sref: 'base.configuration.edit.advanced.models' },
        { text: 'Caches', sref: 'base.configuration.edit.advanced.caches' }
    ];

    menuItems: Array<{text: string, sref: string}>;

    $onInit() {
        this.menuItems = this.constructor.menuItems;
    }
}
