(function() {
    var app = angular.module('tableView', []);
    
    "use strict";
    app.controller('TableViewController', ['$scope', '$http', '$location',
        function($scope, $http, $location){

            $scope.test = "This is a test string";
            $scope.tableData = null;
            $scope.load = true;
            $scope.tableError = false;
            $scope.source_id = $location.search().id;


            function getSource() {
                $http.get('/v1/sources/' + $scope.source_id).success(function (data) {
                    $scope.tableData = data;
                    $scope.load = false;
                    $scope.tableError = false;
                }).error(function (errorData) {
                    $scope.sourcePromise = null;
                    $scope.tableError = errorData;
                });
            };

            getSource();

        }]);
})();
