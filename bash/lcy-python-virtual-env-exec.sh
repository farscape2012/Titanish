#!/usr/bin/env bash
# author : Chengyu Liu

# Exit immediately if a command exits with a non-zero status.
#set -euo pipefail

# Absolute path to this script, e.g. /home/user/bin/foo.sh
FPATH=$(readlink -f "$0")
DIR=$(dirname "$FPATH") # only print a dot (.)
echo "$DIR"

# virtual environment directory
#VirtEnv="$DIR"/.virtEnv/
VirtEnv=~/.virtEnv

virtualenv "$VirtEnv"

source "$VirtEnv/bin/activate"
pip --disable-pip-version-check install --upgrade --requirement ../conf/pypa-requirement > pip-install.log

#pip freeze
#exec  python main.py "$@"
#deactivate

