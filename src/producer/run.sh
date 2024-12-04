#!/bin/bash
cmd=$1

usage() {
    echo "run.sh <command> <arguments>"
    echo "Available commands:"
    echo " register_connector          register a new Kafka connector"
    echo "Available arguments:"
    echo " [connector config path]     path to connector config, for command register_connector only"
}

if [[ -z "$cmd" ]]; then
    echo "Missing command"
    usage
    exit 1
fi

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

case $cmd in
    register_connector)
        if [[ -z "$2" ]]; then
            echo "Missing connector config path"
            usage
            exit 1
        else
            echo "Registering a new connector from $2"
            # First, process the config file to replace environment variables
            processed_config=$(cat "$2" | envsubst)
            
            # Then send the processed config to Kafka Connect
            echo "Processed config:"
            echo "$processed_config"
            echo "$processed_config" | curl -X POST -H 'Content-Type: application/json' --data-binary @- http://localhost:8083/connectors
            
            # Check the response
            if [ $? -eq 0 ]; then
                echo "Connector registered successfully"
            else
                echo "Failed to register connector"
                exit 1
            fi
        fi
        ;;
    generate_schemas)
        # Generate data for 1 device with number of features in the range from 2 to 10
        python generate_schemas.py --min_features 2 --max_features 10 --num_schemas 1
        ;;
    *)
        echo -n "Unknown command: $cmd"
        usage
        exit 1
        ;;
esac
