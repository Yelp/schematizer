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
            case 'source':
                $scope.searchSource($scope.text);
            default: // Should never reach here
        }
    }

    $scope.searchSchema = function(schemaID) {
        var path = '/v1/schemas/' + schemaID;
        $http.get(path).success(function (data) {
            $scope.data = data;
            $scope.show = 'schema';
        }).error(function (errorData) {
            $scope.show = 'error';
            $scope.errorMessage = 'No schema found.'
        });
    }

    $scope.searchTopic = function(topicName) {
        var path = '/v1/topics/' + topicName + '/schemas';
        $http.get(path).success(function (data) {
            $scope.data = data;
            $scope.show = 'topic';
        }).error(function (errorData) {
            $scope.show = 'error';
            $scope.errorMessage = 'No topic found.'
        });
    }

    $scope.searchSource = function(sourceName) {
        var path = '/v1/sources';
        $http.get(path).success(function (data) {
            $scope.data = [];
            for (i in data) {
                if (data[i].source.toLowerCase() === sourceName.toLowerCase()) {
                    $scope.data.push(data[i]);
                }
            }
            if ($scope.data.length > 0) {
                $scope.show = 'sources';
            } else {
                $scope.show = 'error';
                $scope.errorMessage = 'No sources found.'
            }
        }).error(function (errorData) {
            $scope.show = 'error';
            $scope.errorMessage = 'No sources found.'
        });
    }

    $scope.getTopicsFromSource = function(source_id) {
        var path = '/v1/sources/' + source_id + '/topics';
        $http.get(path).success(function (data) {
            $scope.data = data;
            $scope.show = 'source';
        }).error(function (errorData) {
            $scope.show = 'error';
            $scope.show = 'No topics found.'
        });
    }

}]);
