var app = angular.module('docToolApp', []);

app.controller('schemaCtrl', ['$scope', '$http', function($scope, $http) {
    $scope.schema = {};
    $scope.showSchema = false;
    $scope.showSchemaError = false;
    $scope.value = '';

    $scope.searchByID = function() {
        var path = '/v1/schemas/' + $scope.value;
        $http.get(path).success(function (data) {
            $scope.schema.rawSchema = data;
            $scope.schema.id = $scope.value
            $scope.showSchema=true;
            $scope.showSchemaError = false;
        }).error(function (errorData) {
            $scope.schema = {};
            $scope.showSchema = false;
            $scope.showSchemaError = true;
        });
    }
}]);
