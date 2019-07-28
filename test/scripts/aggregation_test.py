#!/usr/bin/python3

import os
import re
import shutil
import time
import vagrant

from fabric import Connection


def clean_vm(root):
    """Remove vagrant VM from specified path"""
    v = vagrant.Vagrant(root=root)
    print(" - Cleanig VM ", root)

    try:
        v.destroy()
    except Exception as err:
        print(err)

    try:
        os.remove(root + "/Vagrantfile")
    except FileNotFoundError:
        pass



def init_vm(vmName, root):
    """Init vagrant VM in given path"""
    buildPath = root + "/build"
    v = vagrant.Vagrant(root=root)

    if not os.path.exists(root):
        os.makedirs(root)

    print(" - Setting up VM ", root)
    if not os.path.exists(buildPath):
        os.makedirs(buildPath)
    v.init(box_name=vmName)

def run_vm(root):
    """Start vagrant VM"""

    v = vagrant.Vagrant(root=root)
    print(" - Starting VM ", root)
    v.up()

def init_test_env(src, root):
    """Create necessary resources in folder associated with test"""
    
    build = root + "/build"
    if not os.path.exists(build):
        os.makedirs(build)
    
    try:
        shutil.rmtree(root + "/build/src")
    except FileNotFoundError:
        pass

    dst = root + "/build/src"
    os.rename(src, dst)

def load_modules(root):
    print(" - Loading modules.")
    v = vagrant.Vagrant(root=root)
    cmd = "modprobe iscsi_tcp"
    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })
    out = con.sudo(cmd)

def register_container_in_vm(root):
    print(" - Adding container to the registry.")
    v = vagrant.Vagrant(root=root)
    cmd = "docker load < ./build/src/_output/joviandss-kubernetes-csi-latest"
    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })
    out = con.sudo(cmd)

def add_secrets(root):
    print(" - Adding configs as secrets.")
    v = vagrant.Vagrant(root=root)
    add_controller_cmd = ("kubectl create secret generic jdss-controller-cfg "
                          "--from-file=./build/controller-cfg.yaml")
    add_node_cmd = ("kubectl create secret generic jdss-node-cfg "
                    "--from-file=./build/node-cfg.yaml")
    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })
    out = con.run(add_controller_cmd)
    out = con.run(add_node_cmd)

def start_plugin(root):
    print(" - Starting plugin.")
    v = vagrant.Vagrant(root=root)

    ctrl = "./build/src/deploy/joviandss/joviandss-csi-controller.yaml"

    node = "./build/src/deploy/joviandss/joviandss-csi-node.yaml"

    replace = "sed -i 's/imagePullPolicy: Always/imagePullPolicy:  IfNotPresent/g' "
    replaceControllerPull = replace + " ./build/src/deploy/joviandss/joviandss-csi-controller.yaml"
    replaceNodePull = replace +       " ./build/src/deploy/joviandss/joviandss-csi-node.yaml"

    addControllerCmd = "kubectl apply -f " + ctrl
    addNodeCmd = "kubectl apply -f " + node

    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })
    out = con.run(replaceControllerPull)
    out = con.run(replaceNodePull)
    out = con.run(addControllerCmd)
    out = con.run(addNodeCmd)

def start_nginx(root):
    print(" - Starting test deployment.")
    v = vagrant.Vagrant(root=root)

    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })
    createPVC = "kubectl apply -f ./build/src/deploy/example/nginx-pvc.yaml"
    start_nginx_cmd = "kubectl apply -f ./build/src/deploy/example/nginx.yaml"
    out = con.run(createPVC)
    out = con.run(start_nginx_cmd)


def wait_for_plugin_started(root, t):
    print(" - Waiting for plugin to start.")
    v = vagrant.Vagrant(root=root)

    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })

    controllerRunning = re.compile(r'^joviandss-csi-controller-0.*3\/3.*Running.*$')
    controllerCreating = re.compile(r'^joviandss-csi-controller-0.*ContainerCreating.*$')
    node_running = re.compile(r'^joviandss-csi-node-.*2\/2.*Running.*$')
    node_creating = re.compile(r'^joviandss-csi-node-.*ContainerCreating.*$')

    while t > 0:
        t = t - 1
        time.sleep(1)
        out = str(con.run("kubectl get pods", hide=True).stdout)

        #if len(out):
        #    continue

        #print("Out: ", out)
        cr = ""
        nr = ""

        for line in out.splitlines():
            cr = controllerRunning.search(line)
            if cr is None:
                continue
            break

        for line in out.splitlines():
            nr = node_running.search(line)
            if nr is None:
                continue
            break

        if cr != None and nr != None:
            return True
        cc = ""
        nc = ""
        for line in out.splitlines():
            cc = controllerCreating.search(line)
            if cc is None:
                continue
            break
        for line in out.splitlines():
            nc = node_creating.search(line)
            if nc is None:
                continue
            break

        if len([i for i in [cc, cr, nc, nr] if i != None]) != 2:
            print([cc, cr, nc, nr])
            out = con.run("kubectl get events")
            raise Exception("Fail during plugin loading.")

    raise Exception("Unable to get plugins to start running in time.")

def wait_for_nginx_started(root, t):
    """Startn nginx container with JovianDSS volume
            and wait till it successfully loaded.
    """

    v = vagrant.Vagrant(root=root)

    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })
    nginx_pending = re.compile(r'^nginx.*Pending.*$')
    nginx_running = re.compile(r'^nginx.*Running.*$')
    nginx_creating = re.compile(r'^nginx.*ContainerCreating.*$')

    while t > 0:
        time.sleep(1)
        t = t - 1
        out = str(con.run("kubectl get pods", hide=True).stdout)

        if len(out) == 0:
            continue
        nr = ""

        for line in out.splitlines():
            nr = nginx_running.search(line)
            if nr is None:
                continue
            return True

        np = None
        for line in out.splitlines():
            np = nginx_pending.search(line)
            if np is None:
                continue
            break

        nc = None
        for line in out.splitlines():
            nc = nginx_creating.search(line)
            if nc is None:
                continue
            break

        if (nc is None) and (np is None):
            print(out)
            out = con.run("kubectl get events")
            raise Exception("Fail during nginx loading.")

    raise Exception("Unable to get nginx to start running in time.")

def create_storage_class(root):
    """Create storage class in test VM"""

    print(" - Creating Storage class.")

    v = vagrant.Vagrant(root=root)

    sc_file = "./build/src/deploy/joviandss/joviandss-csi-sc.yaml"

    add_sc_cmd = "kubectl apply -f " + sc_file

    con = Connection(v.user_hostname_port(),
                     connect_kwargs={
                         "key_filename": v.keyfile(),
                     })
    con.run(add_sc_cmd)

def main():
    """Runs aggregation test on freshly build
            container of kubernetes csi plugin

    Parameters:
    root -- folder to run test in
    csi_test_vm -- name of vagrant VM to run test in
    """
    root = "aggregation-test"
    csi_test_vm = "kubernetes-14.3"
    clean_vm(root)

    init_vm(csi_test_vm, root)
    #init_test_env("./build/src", root)
    try:
        run_vm(root)
        load_modules(root)
        register_container_in_vm(root)
        add_secrets(root)
        start_plugin(root)
        wait_for_plugin_started(root, 220)
        create_storage_class(root)
        start_nginx(root)
        wait_for_nginx_started(root, 120)
    except Exception as err:
        print(err)
        #clean_vm(root)
        raise err

    print("Success!")

if __name__ == "__main__":
    main()
