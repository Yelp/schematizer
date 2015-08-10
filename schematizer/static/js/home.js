(function() {
    var app = angular.module('home', []);

    "use strict";
    app.controller('HomeController', ['$scope', '$http',
        function($scope, $http){

            $scope.tables = [];
            $scope.load = true;
            $scope.schemaFilter = 'public_v1';
            $scope.allCategories = '[ All Categories ]';
            $scope.uncategorized = '[ Uncategorized ]';
            $scope.categoryFilter = $scope.allCategories;
            $scope.categories = [];

            function initBrowse() {
                $http.get('/v1/namespaces/public_v1/sources').success(function (data) {
                    $scope.tables = data;
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