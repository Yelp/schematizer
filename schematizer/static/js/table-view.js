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
            $scope.tableDescription = "";
            $scope.isEditingTableNote = false;
            $scope.tableNoteEdit = "";
            $scope.columnNoteEdit = {};
            $scope.user = "user@yelp.com"; // TODO: (wscheng#DATAPIPE-233): Attach user to this variable once stargate is activated


            // Functions for saving and editing table data
            $scope.editTableNote = function() {
                $scope.isEditingTableNote = true;
                $scope.tableNoteEdit = $scope.tableNote.note;
            };

            $scope.saveTableNote = function() {
                if ($scope.tableNote === undefined) {
                    $http({
                        url: '/v1/notes',
                        method: "POST",
                        data: JSON.stringify({
                            reference_id: parseInt($scope.schema_id),
                            reference_type: 'schema',
                            note: $scope.tableNoteEdit,
                            last_updated_by: $scope.user
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
                            last_updated_by: $scope.user
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

            $scope.editColumnNote = function(field_id) {
                $scope.columnNoteEdit[field_id].isEditing = true;
                $scope.columnNoteEdit[field_id].edit = $scope.columnNoteEdit[field_id].note.note;
            }

            $scope.saveColumnNote = function(field_id) {
                if ($scope.columnNoteEdit[field_id].note === undefined) {
                    $http({
                        url: '/v1/notes',
                        method: "POST",
                        data: JSON.stringify({
                            reference_id: field_id,
                            reference_type: 'schema_element',
                            note: $scope.columnNoteEdit[field_id].edit,
                            last_updated_by: $scope.user
                        }),
                        headers: {'Content-Type': 'application/json'}
                    }).success(function (data) {
                        $scope.columnNoteEdit[field_id].note = data;
                        $scope.columnNoteEdit[field_id].note.note = $scope.columnNoteEdit[field_id].edit;
                    });
                }
                else {
                    $http({
                        url: '/v1/notes/'+ $scope.columnNoteEdit[field_id].note.id,
                        method: "POST",
                        data: JSON.stringify({
                            note: $scope.columnNoteEdit[field_id].edit,
                            last_updated_by: $scope.user
                        }),
                        headers: {'Content-Type': 'application/json'}
                    }).success(function (data) {
                        $scope.columnNoteEdit[field_id].note = data;
                        $scope.columnNoteEdit[field_id].note.note = $scope.columnNoteEdit[field_id].edit;
                    });
                }
                $scope.columnNoteEdit[field_id].isEditing = false;
            }

            $scope.cancelColumnNote = function(field_id) {
                $scope.columnNoteEdit[field_id].isEditing = false;
            }

            $scope.fieldFilter = function(element) {
                if (element.element_type == 'field')
                    return true;
                return false;
            }


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
                    for (var i = 0; i < data.length; i++) {
                        if (data[i].element_type == 'record')
                            $scope.tableDescription = data[i].doc;
                        if (data[i].element_type == 'field')
                            $scope.schemaElements.push(data[i])
                    }
                    for (var i = 0; i < $scope.schemaElements.length; i++) {
                        $scope.columnNoteEdit[$scope.schemaElements[i].id] = {isEditing: false, edit: "", note: $scope.schemaElements[i].note};
                    }
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                });
                $scope.load = false;
            };

            initTable();    // Initialize table data on page load

        }]);
})();
