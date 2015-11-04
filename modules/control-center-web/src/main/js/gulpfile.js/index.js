var gulp = require('gulp');
var gulp_jade = require('./tasks/jade');
var gulp_sass = require('./tasks/sass');

gulp.task('default', ['build']);

gulp.task('build', ['jade', 'sass']);

gulp.task('watch', ['sass:watch', 'jade:watch']);