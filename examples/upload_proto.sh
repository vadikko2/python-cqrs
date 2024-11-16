user_joined_proto=$(cat ./examples/proto/user_joined.proto | jq -Rs .)
echo "Upload schema: $user_joined_proto"

curl -X POST http://localhost:8085/subjects/user_joined_proto-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d '{
  "schemaType": "PROTOBUF",
  "schema": "syntax = \"proto3\";\n\nmessage UserJoinedECST {\n  message Payload {\n    string user_id = 1;\n    string meeting_id = 2;\n  }\n  string event_id = 1;\n  string event_timestamp = 2;\n  string event_name = 3;\n  string event_type = 4;\n  string topic = 5;\n  repeated Payload payload = 6;\n}\n\nmessage UserJoinedNotification {\n  message Payload {\n    string user_id = 1;\n    string meeting_id = 2;\n  }\n  string event_id = 1;\n  string event_timestamp = 2;\n  string event_name = 3;\n  string event_type = 4;\n  string topic = 5;\n  repeated Payload payload = 6;\n}"
}'
