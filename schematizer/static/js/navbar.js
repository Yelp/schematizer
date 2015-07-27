/**
 * @fileOverview - This is a simple module that helps figure out which element
 * of the navigation bar should be considered active / selected.
 *
 * Code taken from:
 * http://stackoverflow.com/questions/16199418/how-do-i-implement-the-bootstrap-navbar-active-class-with-angular-js
 *
 */

(function() {
    "use strict";
    var app = angular.module('navbar', []);

    app.controller('NavbarController', ['$scope', '$location',
        function($scope, $location) {

        $scope.isActive = function (viewLocation) {
            // Do a `startswith` instead of an exact match, so that we can
            // match on prefixes as well. For example, a drop-down menu might
            // contain several links with /prefix/element and it should be
            // selected for all pages that start with /prefix.
            return $location.path().indexOf(viewLocation) == 0;
        };

    } ]);

} )();
