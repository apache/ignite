var gulp       = require('gulp');
var requireDir = require('require-dir');

// Require all tasks in gulpfile.js/tasks, including subfolders
requireDir('./tasks', { recurse: true });

// Summary tasks
gulp.task('default', ['build']);
gulp.task('watch', ['build', 'sass:watch', 'jade:watch', 'copy:watch']);
