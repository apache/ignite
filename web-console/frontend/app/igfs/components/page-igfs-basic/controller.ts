import { Component, OnInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';


export default class PageIgfsBasicComponent implements OnInit {
    static $inject = ['$sanitize','$sce'];

    url = '/filemanager/index.html';

    safeUrl: SafeResourceUrl;

    safeStringUrl: SafeResourceUrl;

    constructor(private $sanitize,private $sce) {
        
        this.safeUrl = this.$sce.trustAsResourceUrl(this.url)
        this.safeStringUrl = this.$sanitize(this.url);
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
        console.log(this.safeUrl);
        console.log(this.safeStringUrl);
    }

    ngAfterViewInit() {            
        // 或平滑滚动（推荐）
        window.scrollBy({
            top: 190,
            behavior: 'smooth'
        });
    }

}
