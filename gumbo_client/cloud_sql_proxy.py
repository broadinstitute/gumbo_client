import subprocess
import socket
import os
import json


def is_pid_valid(pid):
    "returns True if there exists a process with the given PID"
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def is_port_free(port):
    "return true if we expect we can listen to the given TCP port on localhost"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("localhost", port))
    except socket.error:
        return False
    finally:
        s.close()
    return True


def alloc_free_port(start_port, max_tries=20):
    "Returns first free TCP port starting at start_port and throwing an exception after max_tries"
    for i in range(max_tries):
        port = start_port + i
        if is_port_free(port):
            return port
    raise Exception(f"Could not find free port in range {port}-{port+max_tries}")


def get_cloud_sql_proxy_port(pid_file, instance):
    "Starts cloud_sql_proxy if there isn't already an instance running and returns the port its listening on"
    # check for an existing config file
    if os.path.exists(pid_file):
        with open(pid_file, "rt") as fd:
            existing_config = json.load(fd)

        pid = existing_config["pid"]
        if is_pid_valid(pid):
            return existing_config["port"]
        else:
            # if the pid doesn't exist delete this config because it's stale
            print(
                f"Removing stale config file from previous launch of cloud_sql_proxy: {pid_file}"
            )
            os.unlink(pid_file)

    port = alloc_free_port(5432)
    print(f"Starting cloud_sql_proxy for {instance} listening on port {port}")
    proc = subprocess.Popen(["cloud_sql_proxy", f"-instances={instance}=tcp:{port}"])
    with open(pid_file, "wt") as fd:
        fd.write(json.dumps({"pid": proc.pid, "port": port}))
    return port
