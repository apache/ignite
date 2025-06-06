import { Component, OnInit,AfterViewInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import {forkJoin, merge, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import ConfigureState from '../../../configuration/services/ConfigureState';
import ConfigSelectors from '../../../configuration/store/selectors';
import Clusters from '../../../configuration/services/Clusters';

import cloneDeep from 'lodash/cloneDeep';
import {UIRouter} from '@uirouter/angularjs';

import Datasource from 'app/datasource/services/Datasource';

export default class PageDatasetsBasicComponent implements OnInit, AfterViewInit {
    static $inject = ['$sanitize','$sce','$uiRouter','Datasource','ConfigureState','ConfigSelectors'];

    url = 'http://localhost:18080/mongoAdmin/queryDocuments#admin';

    safeUrl: SafeResourceUrl;

    safeStringUrl: SafeResourceUrl;

    constructor(
        private $sanitize,private $sce,
        private $uiRouter: UIRouter,
        private Datasource: Datasource,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors,
    ) {
        
        this.safeUrl = this.$sce.trustAsResourceUrl(this.url);
        this.safeStringUrl = this.$sanitize(this.url);    
    }

    $onInit(){
        this.ngOnInit();
    }

    $postLink(){
        this.ngAfterViewInit();
    }    
    
    ngOnInit() {
        const datasetID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('datasetID'),
            filter((v) => v),
            take(1)
        );

        this.originalCluster$ = datasetID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );
        
        
        this.originalCluster$.subscribe((c) =>{
            this.clonedCluster = cloneDeep(c);
            let jndi_url  = 'mongodb://127.0.0.1:2701/'+this.clonedCluster.name;               
            this.setCookie('currentDatasetUrl',jndi_url,30);
            if (this.clonedCluster.crudui_web_url){
                this.url = this.clonedCluster.crudui_web_url;
                this.safeUrl = this.$sce.trustAsResourceUrl(this.url);
                this.safeStringUrl = this.$sanitize(this.url);   
                console.log(this.safeUrl);
                console.log(this.safeStringUrl); 
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

    setCookie(name: string, value: string, days: number): void {
        const expires = new Date();
        expires.setTime(expires.getTime() + days * 24 * 60 * 60 * 1000);
        const expiresStr = 'expires=' + expires.toUTCString();
        document.cookie = `${name}=${value};${expiresStr};path=/`;
    }

    getCookie(name: string): string | null {
        const cookieArray = document.cookie.split('; ');
        for (let i = 0; i < cookieArray.length; i++) {
            const cookiePair = cookieArray[i].split('=');
            if (name === cookiePair[0]) {
                return decodeURIComponent(cookiePair[1]);
            }
        }
        return null;
    }

    deleteCookie(name: string): void {
        document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 UTC;path=/`;
    }

}
