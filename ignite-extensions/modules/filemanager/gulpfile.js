'use strict';

// Require
var gulp = require('gulp');
var templateCache = require('gulp-angular-templatecache');
var uglify = require('gulp-uglify');
var minifyCss = require('gulp-clean-css');
var concat = require('gulp-concat');
var eslint = require('gulp-eslint');
var del = require('del');
var path = require('path');

// Vars
var src = 'app/';
var dst = 'src/main/resources/static/dist/';
var tplPath = 'app/templates'; //must be same as fileManagerConfig.tplPath
var jsFile = 'angular-filemanager.min.js';
var cssFile = 'angular-filemanager.min.css';

gulp.task('clean', function (cb) {
  del(dst + '/*', cb);
});

gulp.task('cache-templates', function () {
  return gulp.src(tplPath + '/*.html')
    .pipe(templateCache(jsFile, {
      module: 'FileManagerApp',
      base: function(file) {
        return tplPath + '/' + path.basename(file.history.toString());
      }
    }))
    .pipe(gulp.dest(dst));
});

gulp.task('concat-uglify-js', ['cache-templates'], function() {
  return gulp.src([
    src + 'js/app.js',
      src + 'js/*/*.js',
      dst + '/' + jsFile
    ])
    .pipe(concat(jsFile))
    .pipe(uglify())
    .pipe(gulp.dest(dst));
});

gulp.task('minify-css', function() {
  return gulp.src(src + 'css/*.css')
    .pipe(minifyCss({compatibility: 'ie8'}))
    .pipe(concat(cssFile))
    .pipe(gulp.dest(dst));
});

gulp.task('lint', function () {
  return gulp.src([src + 'js/app.js', src + 'js/*/*.js'])
    .pipe(eslint({
      'rules': {
          'quotes': [2, 'single'], 
          //'linebreak-style': [2, 'unix'],
          'semi': [2, 'always']
      },
      'env': {
          'browser': true
      },
      'globals': {
          'angular': true,
          'jQuery': true
      },
      'extends': 'eslint:recommended'
    }))
    .pipe(eslint.format())
    .pipe(eslint.failOnError());
});

gulp.task('default', ['concat-uglify-js', 'minify-css']);
gulp.task('build', ['clean', 'lint', 'default']);