# Get topic from first argument, default to "raw-events-topic" if not provided
TOPIC=${1:-raw-events-topic}

curl --silent -X GET http://localhost:8081/subjects/$TOPIC/versions/latest | jq
