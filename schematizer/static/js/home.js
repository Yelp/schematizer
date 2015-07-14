(function() {
    "use strict";
    var app = angular.module('home', []);

    app.controller('HomeController', ['$scope', '$http',
        function($scope, $http){

            $scope.test = "This is a test string";

        }])
})();