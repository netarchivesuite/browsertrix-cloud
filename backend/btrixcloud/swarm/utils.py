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


def create_config(name, data, labels):
    """ create config from specified data """
    with tempfile.NamedTemporaryFile("wt") as fh_io:
        fh_io.write(data)
        fh_io.flush()

        try:
            docker.config.create(name, fh_io.name, labels=labels)
        except DockerException as exc:
            print(exc, flush=True)


def get_config(name):
    """ get config data, base64 decode """
    try:
        config = docker.config.inspect(name)
        return base64.b64decode(config.spec.data)
    except DockerException as exc:
        print(exc, flush=True)
        return None


def delete_config(name):
    """ get config data, base64 decode """
    try:
        docker.config.remove(name)
        return True
    except DockerException as exc:
        print(exc, flush=True)
        return False


def delete_configs(label):
    """ delete configs with specified label """
    try:
        configs = docker.config.list(filters={"label": label})
        for config in configs:
            config.remove()

    except DockerException as exc:
        print(exc, flush=True)


def get_service(service_name):
    """ get a swarm service """
    try:
        res = docker.service.inspect(service_name)
        return res
    except DockerException:
        return None


def get_service_labels(service_name):
    """ get labels from a swarm service """
    service = get_service(service_name)
    return service.spec.labels if service else {}


def scale_service(service_name, new_scale):
    """ update scale of service """
    service = get_service(service_name)
    if not service:
        print(f"service {service_name} not found", flush=True)
        return False

    try:
        service.scale(new_scale)
    except DockerException as exc:
        print(exc, flush=True)
        return False

    return True


def ping_containers(value, signal_="SIGTERM"):
    """ ping running containers with given service name with signal """
    try:
        conts = docker.container.list(filters={"name": value})
        for cont in conts:
            print("Sending Signal: " + signal_, flush=True)
            cont.kill(signal_)
        return True
    except DockerException as exc:
        print(exc, flush=True)
        return False
