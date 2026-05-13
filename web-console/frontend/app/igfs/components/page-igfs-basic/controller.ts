import {Observable, combineLatest, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import ConfigureState from 'app/configuration/services/ConfigureState';
import { Component, OnInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import {UIRouter} from '@uirouter/angularjs';

export default class PageIgfsBasicComponent implements OnInit {
    static $inject = ['$sanitize','$sce','$window','$uiRouter', 'ConfigureState'];

    url = '/filemanager/';

    safeUrl: SafeResourceUrl;

    safeStringUrl: SafeResourceUrl;

    constructor(private $sanitize,private $sce,private $window,
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState
    ) {
        
        this.safeUrl = this.$sce.trustAsResourceUrl(this.url+'index.html')
        this.safeStringUrl = this.$sanitize(this.safeUrl);
    }

    _loadMongoExpress(id: string) {
        try {            
            const mongoExpress = JSON.parse(localStorage.igfsStorages);
            if (mongoExpress && mongoExpress[id]) {            
                return mongoExpress[id];
            }
        }
        catch (ignored) {
            
        }          
    }
    
    $onInit(){
        this.ngOnInit();
    }

    $postLink(){
        this.ngAfterViewInit();
    }

    ngOnInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('storageID')); 

        this.clusterID$.subscribe((id) =>{
            let c = this._loadMongoExpress(id)
            if (c && (c.url || c.accessMode=='logs' || c.accessMode=='s3')){
                localStorage.currentStorageHost = c.url;
                this.$uiRouter.stateService.go('base.igfs.overview',{},{
                    location: 'replace',  
                    reload: false,      
                    inherit: false
                }).then(()=>{
                    let endpont = c.accessMode || 'index';
                    this.$window.location.href = this.url+ endpont+'.html';
                });
            }
            else{
                this.$uiRouter.stateService.go('base.igfs.edit.advanced',{storageID: id});
            }
        })
    }

    ngAfterViewInit() {            
        // 或平滑滚动（推荐）
        window.scrollBy({
            top: 190,
            behavior: 'smooth'
        });
    }

}
