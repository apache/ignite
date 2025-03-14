import { Component, OnInit } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';

export default class IgfsHomeComponent implements OnInit {
    static $inject = ['$sanitize','$sce'];

    url = 'http://localhost:3000/webapps/mongoAdmin/queryDocuments#admin';

    safeUrl: SafeResourceUrl;

    safeStringUrl: SafeResourceUrl;

    constructor(private $sanitize,private $sce) {
        try {            
            const mongoExpress = this._loadMongoExpress('admin');
            if (mongoExpress && mongoExpress.url) {            
                this.url = mongoExpress.url;
            }
        }
        catch (ignored) {
            // No-op.
        }
        this.safeUrl = this.$sce.trustAsResourceUrl(this.url)
        this.safeStringUrl = this.$sanitize(this.url);
    }

    _loadMongoExpress(id: string) {
        try {            
            const mongoExpress = JSON.parse(localStorage.mongoExpress);
            if (mongoExpress && mongoExpress[id]) {            
                return mongoExpress[id];
            }
        }
        catch (ignored) {
            
        }    
    }

    ngOnInit() {
        console.log(this.safeUrl);
        console.log(this.safeStringUrl);
    }

}
