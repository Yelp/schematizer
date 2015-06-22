(function() {
    "use strict";
    var app = angular.module('browseSchema', []);

    app.controller('BrowseSchemaController', ['$scope', '$http',
        function($scope, $http){

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
                    case 'namespace':
                        $scope.searchNamespace($scope.text);
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
                    $scope.errorMessage = 'No schema found.';
                });
            }

            $scope.searchTopic = function(topicName) {
                var path = '/v1/topics/' + topicName + '/schemas';
                $http.get(path).success(function (data) {
                    $scope.data = data;
                    $scope.show = 'topic';
                }).error(function (errorData) {
                    $scope.show = 'error';
                    $scope.errorMessage = 'No topic found.';
                });
            }

            $scope.searchSource = function(sourceName) {
                var path = '/v1/sources';
                $http.get(path).success(function (data) {
                    $scope.data = [];
                    for (var i in data) {
                        if (data[i].source.toLowerCase() === sourceName.toLowerCase()) {
                            $scope.data.push(data[i]);
                        }
                    }
                    if ($scope.data.length > 0) {
                        $scope.show = 'sources';
                    } else {
                        $scope.show = 'error';
                        $scope.errorMessage = 'No sources found.';
                    }
                }).error(function (errorData) {
                    $scope.show = 'error';
                    $scope.errorMessage = 'No sources found.';
                });
            }

            $scope.getTopicsFromSource = function(source_id) {
                var path = '/v1/sources/' + source_id + '/topics';
                $http.get(path).success(function (data) {
                    $scope.data = data;
                    if ($scope.data.length > 0) {
                        $scope.show = 'source';
                    } else {
                        $scope.show = 'error';
                        $scope.errorMessage = 'No sources found.';
                    }
                }).error(function (errorData) {
                    $scope.show = 'error';
                    $scope.show = 'No topics found.';
                });
            }

            $scope.searchNamespace = function(namespace) {
                var path = '/v1/namespaces';
                $http.get(path).success(function (data) {
                    $scope.data = [];
                    for (var i in data) {
                        // Check if data[i] contains substring namespace
                        if (data[i].indexOf(namespace) >= 0) {
                            $scope.data.push(data[i]);
                        }
                    }
                    if ($scope.data.length > 0) {
                        $scope.show = 'namespaces';
                    } else {
                        $scope.show = 'error';
                        $scope.errorMessage = 'No namespaces found.';
                    }
                }).error(function (errorData) {
                    $scope.show = 'error';
                    $scope.show = 'No namespaces found.';
                });
            }

            $scope.getSourcesFromNamespace = function(namespace) {
                var path = '/v1/namespaces/' + namespace + '/sources';
                $http.get(path).success(function (data) {
                    $scope.data = data;
                    $scope.show = 'namespace';
                }).error(function (errorData) {
                    $scope.show = 'error';
                    $scope.show = 'No sources found.';
                });
            }

        }])
})();