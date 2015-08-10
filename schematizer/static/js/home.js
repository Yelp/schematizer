(function() {
    var app = angular.module('home', []);

    "use strict";
    app.controller('HomeController', ['$scope', '$http',
        function($scope, $http){

            $scope.tables = [];
            $scope.filtered = [];
            $scope.load = true;
            $scope.schemaFilter = 'public_v1';
            $scope.allCategories = '[ All Categories ]';
            $scope.uncategorized = '[ Uncategorized ]';
            $scope.categoryFilter = $scope.allCategories;
            $scope.categories = [];

            $scope.tableFilter = function(table) {
                if ($scope.categoryFilter == $scope.allCategories) {
                    return true;
                }
                else if ($scope.categoryFilter == $scope.uncategorized) {
                    return table.category == undefined;
                }
                return table.category == $scope.categoryFilter;
            }

            function initBrowse() {
                $http.get('/v1/namespaces/public_v1/sources').success(function (data) {
                    $scope.tables = data;
                    $scope.filtered = data;
                    $scope.load = false;
                }).error(function (errorData) {
                    $scope.error = errorData;
                    $scope.load = false;
                });
                $http.get('/v1/categories').success(function (data) {
                    $scope.categories = data;
                });
            };

            initBrowse();

        }]);
})();