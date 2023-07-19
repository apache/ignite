

export type QueryActions < T > = Array<{text: string, click?(item: T): any, available?(item: T): boolean}>;

export default class QueryActionButton<T> {
    static $inject = ['$element'];

    item: T;

    actions: QueryActions<T>;

    boundActions: QueryActions<undefined> = [];

    constructor(private el: JQLite) {}

    $postLink() {
        this.el[0].classList.add('btn-ignite-group');
    }

    $onChanges(changes: {actions: ng.IChangesObject<QueryActionButton<T>['actions']>}) {
        if ('actions' in changes) {
            this.boundActions = changes.actions.currentValue.map((a) => {
                const action = {...a};

                const click = () => a.click(this.item);

                Object.defineProperty(action, 'click', {
                    get: () => {
                        return typeof a.available === 'function'
                            ? a.available(this.item) ? click : void 0
                            : a.available ? click : void 0;
                    }
                });
                return action;
            });
        }
    }
}
