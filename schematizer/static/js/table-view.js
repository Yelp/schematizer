(function() {
    var app = angular.module('tableView', []);

    "use strict";
    app.controller('TableViewController', ['$scope', '$http', '$location',
        function($scope, $http, $location){

            $scope.tableData = null;
            $scope.load = true;
            $scope.tableError = false;
            $scope.source_id = $location.search().id;
            $scope.schema_id = null;
            $scope.tableNote = "";
            $scope.schemaElements = [];
            $scope.isEditingTableNote = false;
            $scope.tableNoteEdit = "";
            $scope.user = "user@yelp.com"; // TODO: Set user through stargate BAM-

            // Functions for saving and editing table data
            $scope.editTableNote = function() {
                $scope.isEditingTableNote = true;
                $scope.tableNoteEdit = $scope.tableNote.note;
            };

            $scope.saveTableNote = function(note) {
                if ($scope.tableNote === null) {
                    $http({
                        url: '/v1/notes',
                        method: "POST",
                        data: JSON.stringify({
                            reference_id: parseInt($scope.schema_id),
                            reference_type: 'schema',
                            note: $scope.tableNoteEdit,
                            last_updated_by: 'wscheng@yelp.com'
                        }),
                        headers: {'Content-Type': 'application/json'}
                    }).success(function (data) {
                        $scope.tableNote = data;
                        $scope.tableNote.note = $scope.tableNoteEdit;
                    });
                }
                else {
                    $http({
                        url: '/v1/notes/'+ $scope.tableNote.id,
                        method: "POST",
                        data: JSON.stringify({
                            note: $scope.tableNoteEdit,
                            last_updated_by: 'wscheng@yelp.com'
                        }),
                        headers: {'Content-Type': 'application/json'}
                    }).success(function (data) {
                        $scope.tableNote = data;
                        $scope.tableNote.note = $scope.tableNoteEdit;
                    });
                }
                $scope.isEditingTableNote = false;
            };

            $scope.cancelTableNote = function() {
                $scope.isEditingTableNote = false;
            };


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
                    $scope.tableNote = data.note;
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

            initTable();    // Initialize table data on page load

        }]);
})();
