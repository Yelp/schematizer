(function() {
    var docToolApp = angular.module('docToolApp', [
        'ngRoute',
        'navbar',
        'home',
        'browseTables',
        'browseSchema'
    ]);

    "use strict";
    docToolApp.config(['$routeProvider',
        function($routeProvider) {
            $routeProvider.
            when('/home', {
                templateUrl: 'partials/home.html',
                controller: 'HomeController'
            }).
            when('/browse', {
                templateUrl: 'partials/browse-tables.html',
                controller: 'BrowseTablesController'
            }).
            when('/schema', {
                templateUrl: 'partials/browse-schema.html',
                controller: 'BrowseSchemaController'
            }).
            otherwise({
                redirectTo: '/home'
            });
        }
    ]);
})();
