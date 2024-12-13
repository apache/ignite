

import get from 'lodash/get';
import {Observable, combineLatest, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import ConfigureState from 'app/configuration/services/ConfigureState';
import {UIRouter} from '@uirouter/angularjs';

export default class PageDatasetsController {
    static $inject = ['$uiRouter', 'ConfigureState'];

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState
    ) {}

    clusterID$: Observable<string>;
    clusterName$: Observable<string>;
    tooltipsVisible = true;

    $onInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'));
        
        let cluster$ = this.clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return from(of(this._loadMongoExpress(id)));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );  
               

        const isNew$ = this.clusterID$.pipe(map((v) => v === 'new'));

        this.clusterName$ = combineLatest(cluster$, isNew$, (cluster, isNew) => {
            return `${isNew ? 'Create' : 'Edit'} DataSets configuration 
            ${isNew ? '' : `‘${get(cluster, 'clusterName','')}’`}`;
        });
    }

    _loadMongoExpress(id: string) {
        try {            
            const mongoExpress = localStorage.mongoExpress || "http://localhost:9090/phpMongoAdmin";
            return {id: id, clusterName: "Mongo Express", url: mongoExpress};
        }
        catch (ignored) {
            return {id: id, clusterName: "Mongo Express", url: "http://localhost:9090/phpMongoAdmin"}
        }
    }

    $onDestroy() {}
}
