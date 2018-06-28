const Jasmine = require('jasmine');

const jasmine = new Jasmine();
jasmine.loadConfig({
    'spec_dir': 'spec',
    'spec_files': [
        `examples/${process.argv[2]}.spec.js`
    ],
    "random": false
});
jasmine.execute();