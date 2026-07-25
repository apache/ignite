

export default class PageConfigureAdvancedController {
    static menuItems = [
        { text: 'Cluster', sref: 'base.console.edit.advanced.cluster' },
        { text: 'SQL Scheme', sref: 'base.console.edit.advanced.models' },
        { text: 'Caches', sref: 'base.console.edit.advanced.caches' }       
    ];

    menuItems: Array<{text: string, sref: string}>;

    $onInit() {
        this.menuItems = this.constructor.menuItems;
    }
}
