var app = angular.module('docToolApp', []);

app.controller('schemaCtrl', function($scope) {
    $scope.schema = {id: 1, rawSchema: '{"status": "RW", "created_at": "1969-12-31T16:00:03", "updated_at": "1969-12-31T16:00:03", "topic": {"updated_at": "1969-12-31T16:00:00", "source": {"created_at": "1969-12-31T16:00:01", "namespace": "yelp", "updated_at": "1969-12-31T16:00:01", "source_owner_email": "bam@yelp.com", "source": "business", "source_id": 1}, "created_at": "1969-12-31T16:00:00", "name": "yelp.business.v1", "topic_id": 2}, "schema_id": 1, "schema": "test schema"}'};
    $scope.showSchema = 1;
});
