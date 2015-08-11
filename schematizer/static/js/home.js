(function() {
    var app = angular.module('home', []);

    "use strict";
    app.controller('HomeController', ['$scope', '$http', '$location',
        function($scope, $http, $location){

            $scope.tables = [];
            $scope.filtered = {};
            $scope.load = true;
            $scope.schemaFilter = $location.search().schema;
            $scope.ALL_CATEGORIES = '[ All Categories ]';
            $scope.UNCATEGORIZED = '[ Uncategorized ]';
            $scope.categoryFilter = $scope.ALL_CATEGORIES;
            $scope.categories = [];

            $scope.tableFilter = function(table) {
                if ($scope.categoryFilter == $scope.ALL_CATEGORIES) {
                    return true;
                }
                else if ($scope.categoryFilter == $scope.UNCATEGORIZED) {
                    return table.category == undefined;
                }
                return table.category == $scope.categoryFilter;
            }

            $scope.newSchema = function() {
                $location.search('schema', $scope.schemaFilter);
            }

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
            }

            function initBrowse() {
                $scope.updateSchema();
                $http.get('/v1/categories').success(function (data) {
                    $scope.categories = data;
                });
                $http.get('/v1/namespaces').success(function (data) {
                    $scope.schemas = data;
                });
            };

            initBrowse();

        }]);
})();