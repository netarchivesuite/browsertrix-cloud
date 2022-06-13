#!/bin/bash
export REGISTRY=localhost:5000/
docker-compose build backend frontend
docker stack deploy -c docker-compose.yml -c docker-compose.swarm.yml
