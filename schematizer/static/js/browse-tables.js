(function() {
    "use strict";
    var app = angular.module('browseTables', []);

    app.controller('BrowseTablesController', ['$scope', '$http',
        function($scope, $http){

            $scope.test = "This is a test string";

        }])
})();