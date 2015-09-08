(function() {
    var app = angular.module('tableView', []);

    "use strict";
    app.controller('TableViewController', ['$scope', '$http', '$location', 'CONSTANTS', 'DocToolService',
        function($scope, $http, $location, CONSTANTS, DocToolService){

            $scope.tableData = null;
            $scope.load = true;
            $scope.tableError = false;
            $scope.namespace = $location.search().schema;
            $scope.source = $location.search().table;
            $scope.schema_id = null;
            $scope.tableNote = "";
            $scope.schemaElements = [];
            $scope.schemaElementMetadata = {};
            $scope.tableDescription = "";
            $scope.category = "";
            $scope.categories = [];
            $scope.isCreatingNewCategory = false;
            $scope.isEditingCategory = false;
            $scope.isEditingTableNote = false;
            $scope.tableNoteEdit = "";
            $scope.columnNoteEdit = {};
            $scope.UNCATEGORIZED = CONSTANTS.uncategorized;
            $scope.user = window.user_email;
            $scope.last_updated = "";

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
            };

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
            };

            $scope.cancelColumnNote = function(field_id) {
                $scope.columnNoteEdit[field_id].isEditing = false;
            };

            $scope.editCategory = function() {
                $scope.isEditingCategory = true;
            };

            $scope.saveCategory = function() {
                if ($scope.category == $scope.UNCATEGORIZED || $scope.category == "") {
                    var complete = function (data) {
                        $scope.category = $scope.UNCATEGORIZED;
                        $scope.tableData.category = $scope.category;
                    };
                    $http({
                        url: '/v1/sources/' + $scope.tableData.source_id + '/category',
                        method: "DELETE",
                        headers: {'Content-Type': 'application/json'}
                    }).success(complete)
                        .error(complete);
                } else {
                    $http({
                        url: '/v1/sources/' + $scope.tableData.source_id + '/category',
                        method: "POST",
                        data: JSON.stringify({
                            category: $scope.category
                        }),
                        headers: {'Content-Type': 'application/json'}
                    }).success(function (data) {
                        $scope.tableData.category = $scope.category;
                    });
                }
                $scope.isEditingCategory = false;
                $scope.isCreatingNewCategory = false;
            };

            $scope.newCategory = function() {
                $scope.isCreatingNewCategory = true;
            };

            $scope.fieldFilter = function(element) {
                return element.element_type == 'field';
            };

            $scope.formatDate = function(date) {
                if (date !== undefined) {
                    return date.split('T')[0];
                }
            };

            $scope.formatSchema = function(schema) {
                return DocToolService.formatSchema(schema);
            };


            function initTable() {
                $http.get('/v1/namespaces/' + $scope.namespace + '/sources').success(function (data) {
                    for (var i = 0; i < data.length; i++) {
                        if (data[i].source == $scope.source) {
                            $scope.tableData = data[i];
                            if ($scope.tableData.category !== undefined) {
                                $scope.category = $scope.tableData.category;
                            } else {
                                $scope.category = $scope.UNCATEGORIZED;
                            }
                            getTopic();
                            return;
                        }
                    }
                    $scope.tableError = "Source does not exist";
                    $scope.load = false;
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                    $scope.load = false;
                });
                $http.get('/v1/categories').success(function (data) {
                    $scope.categories = data;
                });
            }

            function getTopic() {
                $http.get('/v1/sources/' + $scope.tableData.source_id + '/topics/latest').success(function (data) {
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
                    $scope.last_updated = $scope.tableData.updated_at;
                    if (data.note !== undefined && $scope.last_updated < data.note.updated_at) {
                        $scope.last_updated = data.note.updated_at;
                    }
                    var fieldData = JSON.parse(data.schema).fields;
                    for (var i = 0; i < fieldData.length; i++) {
                        $scope.schemaElementMetadata[fieldData[i].name] = getColumnType(fieldData[i]);
                    }
                    getSchemaElements();
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                    $scope.load = false;
                });
            }

            function getSchemaElements() {
                $http.get('/v1/schemas/' + $scope.schema_id + '/elements').success(function (data) {
                    for (var i = 0; i < data.length; i++) {
                        if (data[i].element_type == 'record')
                            $scope.tableDescription = data[i].doc;
                        if (data[i].element_type == 'field') {
                            data[i].name = getColumnName(data[i].key);
                            data[i].type = $scope.schemaElementMetadata[data[i].name];
                            $scope.schemaElements.push(data[i]);
                        }
                        if (data[i].updated_at > $scope.last_updated) {
                            $scope.last_updated = data[i].updated_at;
                        }
                        if (data[i].note !== undefined) {
                            if (data[i].note.updated_at > $scope.last_updated) {
                                $scope.last_updated = data[i].note.updated_at;
                            }
                        }
                    }
                    for (var i = 0; i < $scope.schemaElements.length; i++) {
                        $scope.columnNoteEdit[$scope.schemaElements[i].id] = {
                            isEditing: false,
                            edit: "",
                            note: $scope.schemaElements[i].note
                        };
                    }
                }).error(function (errorData) {
                    $scope.tableError = errorData;
                });
                $scope.load = false;
            }

            function getColumnType(metadata) {
                // TODO(psuben#BAM-407|2015-09-08):
                // Handle fixed lengths for types like char(10)
                var type = "";
                if (typeof metadata.type == 'string') {
                    type = metadata.type;
                    type += formatMetaType(metadata);
                } else if (Object.prototype.toString.call(metadata.type) == '[object Array]') {
                    for(var i = 0; i < metadata.type.length; i++) {
                        type += formatArrayMetaType(metadata, i);
                    }
                } else {
                    type = metadata.type;
                }
                return type;
            }

            function formatMetaType(metadata) {
                var resultType = "";
                if (metadata.type == 'string' && metadata.maxlen !== undefined) {
                    resultType += ('(' + metadata.maxlen + ')');
                }
                resultType += ' not null';
                return resultType;
            }

            function formatArrayMetaType(metadata, i) {
                if (metadata.type[i] == 'string' && metadata.maxlen !== undefined) {
                    return ('string(' + metadata.maxlen + ') ');
                } else {
                    return (metadata.type[i] + ' ');
                }
            }

            function getColumnName(name) {
                return name.split('|')[1];
            }

            initTable();    // Initialize table data on page load

        }]);
})();
