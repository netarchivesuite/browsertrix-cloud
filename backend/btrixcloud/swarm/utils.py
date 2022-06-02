""" swarm util functions """

import tempfile
import os
import base64

from python_on_whales import docker


def get_templates_dir():
    """ return directory containing templates for loading """
    return os.path.join(os.path.dirname(__file__), "templates")


def run_swarm_stack(name, data):
    """ run swarm stack via interpolated file """
    with tempfile.NamedTemporaryFile("wt") as fh_io:
        fh_io.write(data)
        fh_io.flush()

        docker.stack.deploy(name, compose_files=[fh_io.name], orchestrator="swarm")

    return name


def delete_swarm_stack(name):
    """ remove stack """
    return docker.stack.remove(name)


# pylint: disable=broad-except
def get_service_labels(service_name):
    """ get labels from a swarm service """
    try:
        res = docker.service.inspect(service_name)
        # print("labels", res.spec.labels)
        return res.spec.labels
    except Exception as exc:
        print(exc, flush=True)
        return {}


def ping_containers(name, value, signal_="SIGTERM"):
    """ ping running containers with signal """
    conts = docker.container.list(all=False, filters={name: value})
    print(name, value)
    print(conts)
    for cont in conts:
        print("Sending Signal", flush=True)
        cont.kill(signal_)


def random_suffix():
    """ generate suffix for unique container """
    return base64.b32encode(os.urandom(5)).lower().decode("utf-8")
