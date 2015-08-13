(function() {
    var docToolApp = angular.module('docToolApp', [
        'ngRoute',
        'navbar',
        'home',
        'tableView'
    ]);

    docToolApp.constant('CONSTANTS', {
        allCategories: '[ All Categories ]',
        uncategorized: '[ Uncategorized ]',
        defaultSchema: 'yelp_dw_redshift.public'
    });

    docToolApp.service('DocToolService', function() {
        return {
            formatSchema: function(schema) {
                // If the string is in the format aaa.bbb, return aaa.
                // If there is no '.', return the original string.
                var name = schema.split('.');
                return name[name.length - 1];
            }
        };
    });

    "use strict";
    docToolApp.config(['$routeProvider', 'CONSTANTS',
        function($routeProvider, CONSTANTS) {
            $routeProvider.
            when('/home', {
                templateUrl: 'partials/home.html',
                controller: 'HomeController'
            }).
            when('/table', {
                templateUrl: 'partials/table-view.html',
                controller: 'TableViewController'
            }).
            when('/about', {
                templateUrl: 'partials/about.html',
                controller: 'AboutController'
            }).
            otherwise({
                redirectTo: '/home'
            });
        }
    ]);
})();
