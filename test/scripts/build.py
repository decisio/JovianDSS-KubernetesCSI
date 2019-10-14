#!/usr/bin/python3

import argparse
import os
import re
import shutil
import time
import vagrant
import sys

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



def init_vm(name, root):
    """Init vagrant VM in given path"""
    build_path = root + "/src"
    v = vagrant.Vagrant(root=root)

    if not os.path.exists(root):
        os.makedirs(root)

    print(" - Setting up VM ", root)
    if not os.path.exists(build_path):
        os.makedirs(build_path)
    v.init(box_name=name)

def run_vm(root):
    """Start vagrant VM"""

    v = vagrant.Vagrant(root=root)
    print(" - Starting VM ", root)
    v.up()

def init_env(src, root):
    """Create necessary resources in folder associated with test"""

    build = root + "/src"

    shutil.rmtree(build)
    os.makedirs(build)

def get_code(root):
    v = vagrant.Vagrant(root=root)

    # Start plugin
    cmd = "git clone --single-branch --branch dev https://github.com/open-e/JovianDSS-KubernetesCSI ~/go/src/JovianDSS-KubernetesCSI"
    con = Connection(v.user_hostname_port(),
        connect_kwargs={
        "key_filename": v.keyfile(),
        })
    out = con.sudo(cmd)

def get_version(src):
    """Get version of currently builded code """
    get_tag = "git -C " + src + " describe --long --tags"
    get_branch = "git -C " + src + "rev-parse --abbrev-ref HEAD"
    tag_out = subprocess.check_output(get_tag)
    branch_out = subprocess.check_output(get_branch)

    return branch_out + "-" + tag_out

def build_code(root, version):
    v = vagrant.Vagrant(root=root)

    # Start plugin
    cmd = "cd ./go/src/JovianDSS-KubernetesCSI; make joviandss-container;"
    con = Connection(v.user_hostname_port(),
        connect_kwargs={
        "key_filename": v.keyfile(),
        })
    out = con.run(cmd)

    cmd = ("sudo docker save -o ~/go/src/JovianDSS-KubernetesCSI/_output/joviandss-csi:" 
            + version + " opene/joviandss-csi:latest")
    con = Connection(v.user_hostname_port(),
        connect_kwargs={
        "key_filename": v.keyfile(),
        })
    out = con.sudo(cmd)



def main(args):
    """Runs aggregation test on freshly build
            container of kubernetes csi plugin

    Parameters:
    root -- folder to run test in
    csi_test_vm -- name of vagrant VM to run test in
    """


    root = "build"
    csi_test_vm = args.bvm

    clean_vm(root)

    clean_vm(root)

    init_vm(csi_test_vm, root)
    #init_env("./build/", root)
    version = get_version("./build/src")
    try:
        run_vm(root)
        #get_code(root)
        build_code(root, version)
    except Exception as err:
        print(err)

        raise err

    if args.nc == False:
        clean_vm(root)
    print("Success!")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--no-clean', dest='nc', type=bool, default=False, 
            help='Do Not clean environment after execution.')
    parser.add_argument('--build-vm', dest='bvm', type=str, default="fedora29-build-env", 
            help='VM template to be used for building plugin.')
    parser.add_argument('--branch', dest='branch', type=str, default="master", 
            help='VM template to be used for building plugin.')

    args = parser.parse_args()
    main(args)
