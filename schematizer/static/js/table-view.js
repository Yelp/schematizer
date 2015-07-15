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
            $scope.topic = null;
            $scope.schema_id = null;
            $scope.schemaElements = [];


            function initTable() {
                $http.get('/v1/sources/' + $scope.source_id).success(function (data) {
                    $scope.tableData = data;
                    getTopic();
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                    $scope.load = false;
                });
            };

            function getTopic() {
                $http.get('/v1/sources/' + $scope.source_id + '/topics/latest').success(function (data) {
                    $scope.topic = data.name; 
                    getSchema();
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                    $scope.load = false;
                });
            }

            function getSchema() {
                $http.get('/v1/topics/' + $scope.topic + '/schemas/latest').success(function (data) {
                    $scope.schema_id = data.schema_id;
                    getSchemaElements();
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                    $scope.load = false;
                });
            };

            function getSchemaElements() {
                $http.get('/v1/schemas/' + $scope.schema_id + '/elements').success(function (data) {
                    $scope.schemaElements = data;
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                });
                $scope.load = false;
            };

            initTable();

        }]);
})();
