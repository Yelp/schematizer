(function() {
    "use strict";
    var app = angular.module('tableView', []);

    app.controller('TableViewController', ['$scope', '$http', '$location',
        function($scope, $http, $location){

            $scope.test = "This is a test string";
            $scope.source_id = $location.search().id;

        }])
})();
