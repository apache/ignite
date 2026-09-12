import { Component, OnInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';


export default class IgfsChinaMapComponent implements OnInit {
    static $inject = ['$sanitize','$sce'];

    url = '/libs/assets/china.html';

    safeUrl: SafeResourceUrl;

    safeStringUrl: SafeResourceUrl;

    constructor(private $sanitize,private $sce) {
        this.safeUrl = this.$sce.trustAsResourceUrl(this.url)
        this.safeStringUrl = this.$sanitize(this.url);
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
