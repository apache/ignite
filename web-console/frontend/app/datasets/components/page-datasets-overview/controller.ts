import { Component, OnInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';

export default class DatasetsHomeComponent implements OnInit {
    static $inject = ['$sanitize','$sce'];

    url = 'http://127.0.0.1:37017';

    safeUrl: SafeResourceUrl;

    safeStringUrl: SafeResourceUrl;

    constructor(private $sanitize,private $sce) {
        try {            
            const mongoExpress = JSON.parse(localStorage.mongoExpress);
            this.url = mongoExpress.url;
        }
        catch (ignored) {
            // No-op.
        }
        this.safeUrl = this.$sce.trustAsResourceUrl(this.url)
        this.safeStringUrl = this.$sanitize(this.url);
    }

    ngOnInit() {
        console.log(this.safeUrl);
        console.log(this.safeStringUrl);
    }

}
