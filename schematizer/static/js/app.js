var app = angular.module('docToolApp', []);

app.controller('docToolCtrl', ['$scope', '$http', function($scope, $http) {
    $scope.data = [];
    $scope.show = '';
    $scope.showSchemaError = false;
    $scope.text = '';
    $scope.control = 'schema';
    $scope.showTopic = false;

    $scope.search = function() {
        switch ($scope.control) {
            case 'schema':
                $scope.searchSchema($scope.text);
                break;
            case 'topic':
                $scope.searchTopic($scope.text);
                break;
            default: // Should never reach here
        }
    }

    $scope.searchSchema = function(schemaID) {
        var path = '/v1/schemas/' + schemaID;
        $http.get(path).success(function (data) {
            $scope.data = data;
            $scope.show = 'schema';
        }).error(function (errorData) {
            $scope.show = 'schemaError';
        });
    }

    $scope.searchTopic = function(topicName) {
        var path = '/v1/topics/' + topicName + '/schemas';
        $http.get(path).success(function (data) {
            $scope.data = data;
            $scope.show = 'topic';
        }).error(function (errorData) {
            $scope.show = 'topicError';
        });
    }

}]);
