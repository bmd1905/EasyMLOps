#!/bin/bash

function setup_permissions() {
    # Create and set permissions for Ray directory
    sudo mkdir -p /tmp/ray
    sudo chmod 777 /tmp/ray
}

function start_cluster() {
    setup_permissions
    docker compose -f docker-compose.ray.yaml up -d --build
    echo "Waiting for Ray cluster to be ready..."
    sleep 10
}

function stop_cluster() {
    docker compose -f docker-compose.ray.yaml down
    # Cleanup Ray directory
    sudo rm -rf /tmp/ray/*
}

function check_status() {
    docker compose -f docker-compose.ray.yaml exec ray-head ray status
}

case "$1" in
    "start")
        start_cluster
        ;;
    "stop")
        stop_cluster
        ;;
    "status")
        check_status
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 1
        ;;
esac
