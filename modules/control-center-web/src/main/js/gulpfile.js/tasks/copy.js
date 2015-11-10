var gulp     = require('gulp');
var sequence = require('gulp-sequence');

var paths = [
    './app/**/**/*.js',
    './controllers/*.js',
    './controllers/**/*.js',
    './controllers/**/*.json'
];

gulp.task('copy', function() {
    return gulp.src(paths)
        .pipe(gulp.dest('./build'))
});

gulp.task('copy:watch', function() {
    gulp.watch(paths, function(e) {
        sequence('copy', 'inject:plugins:js')()
    })
})