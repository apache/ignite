// exports.config = {
//   specs: ['test/e2e/*.js'],
//   capabilities: {

//   }
// };

exports.config = {
  seleniumAddress: 'http://localhost:4444/wd/hub',

  capabilities: {
    'browserName': 'chrome'
    // 'browserName': 'phantomjs',

    // /* 
    //  * Can be used to specify the phantomjs binary path.
    //  * This can generally be ommitted if you installed phantomjs globally.
    //  */
    // 'phantomjs.binary.path': require('phantomjs').path,

    // /*
    //  * Command line args to pass to ghostdriver, phantomjs's browser driver.
    //  * See https://github.com/detro/ghostdriver#faq
    //  */
    // 'phantomjs.ghostdriver.cli.args': ['--loglevel=DEBUG']
  },

  specs: ['test/e2e/*.js'],

  jasmineNodeOpts: {
    showColors: true
  }
};