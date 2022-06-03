""" swarm util functions """

import tempfile
import os
import base64

from python_on_whales import docker
from python_on_whales.exceptions import DockerException


def get_templates_dir():
    """ return directory containing templates for loading """
    return os.path.join(os.path.dirname(__file__), "templates")


def run_swarm_stack(name, data):
    """ run swarm stack via interpolated file """
    with tempfile.NamedTemporaryFile("wt") as fh_io:
        fh_io.write(data)
        fh_io.flush()

        try:
            docker.stack.deploy(name, compose_files=[fh_io.name], orchestrator="swarm")
        except DockerException as exc:
            print(exc, flush=True)

    return name


def delete_swarm_stack(name):
    """ remove stack """
    try:
        docker.stack.remove(name)
        return True
    except DockerException as exc:
        print(exc, flush=True)
        return False


def get_service_labels(service_name):
    """ get labels from a swarm service """
    try:
        res = docker.service.inspect(service_name)
        # print("labels", res.spec.labels)
        return res.spec.labels
    except DockerException as exc:
        print(exc, flush=True)
        return {}


def ping_containers(name, value, signal_="SIGTERM"):
    """ ping running containers with signal """
    try:
        conts = docker.container.list(filters={name: value})
        for cont in conts:
            print("Sending Signal: " + signal_, flush=True)
            cont.kill(signal_)
        return True
    except DockerException as exc:
        print(exc, flush=True)
        return False


def random_suffix():
    """ generate suffix for unique container """
    return base64.b32encode(os.urandom(5)).lower().decode("utf-8")
