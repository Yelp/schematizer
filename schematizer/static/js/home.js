(function() {
    var app = angular.module('home', []);

    "use strict";
    app.controller('HomeController', ['$scope', '$http', '$location', 'CONSTANTS', 'DocToolService',
        function($scope, $http, $location, CONSTANTS, DocToolService){

            $scope.tables = [];
            $scope.filtered = {};
            $scope.load = true;
            $scope.schemaFilter = $location.search().schema;
            if ($scope.schemaFilter == undefined) {
                $scope.schemaFilter = CONSTANTS.defaultSchema;
            }
            $scope.ALL_CATEGORIES = CONSTANTS.allCategories;
            $scope.UNCATEGORIZED = CONSTANTS.uncategorized;
            $scope.categoryFilter = $scope.ALL_CATEGORIES;
            $scope.categories = [];

            $scope.uncategorizedFilter = function(table) {
                return table.category == undefined;
            };

            $scope.tableFilter = function(table) {
                if ($scope.categoryFilter == $scope.UNCATEGORIZED) {
                    return table.category == undefined;
                }
                return table.category == $scope.categoryFilter;
            };

            $scope.newSchema = function() {
                $location.search('schema', $scope.schemaFilter);
            };

            $scope.updateSchema = function() {
                $scope.load = true;
                $http.get('/v1/namespaces/' + $scope.schemaFilter + '/sources').success(function (data) {
                    $scope.tables = data;
                    $scope.filtered = data;
                    $scope.load = false;
                }).error(function (errorData) {
                    $scope.error = errorData;
                    $scope.load = false;
                });
            };

            $scope.formatSchema = function(schema) {
                return DocToolService.formatSchema(schema);
            };


            function initBrowse() {
                $scope.updateSchema();
                $http.get('/v1/categories').success(function (data) {
                    $scope.categories = data;
                });
                $http.get('/v1/namespaces').success(function (data) {
                    $scope.schemas = data;
                });
            }

            initBrowse();

        }]);
})();