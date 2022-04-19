#!/bin/bash

# Simple bash script to build basic lndinit for all the platforms we support
# with the golang cross-compiler.
#
# Copyright (c) 2016 Company 0, LLC.
# Use of this source code is governed by the ISC
# license.

set -e

PKG="github.com/lightninglabs/lndinit"
PACKAGE=lndinit

# green prints one line of green text (if the terminal supports it).
function green() {
  echo -e "\e[0;32m${1}\e[0m"
}

# red prints one line of red text (if the terminal supports it).
function red() {
  echo -e "\e[0;31m${1}\e[0m"
}

# build_release builds the actual release binaries.
#   arguments: <version-tag> <build-system(s)> <ldflags>
function build_release() {
  local tag=$1
  local sys=$2
  local ldflags=$3

  green " - Packaging vendor"
  go mod vendor
  tar -czf vendor.tar.gz vendor

  tag=$(echo "$tag" | sed 's/\//_/')
  maindir=$PACKAGE-$tag
  mkdir -p $maindir

  cp vendor.tar.gz $maindir/
  rm vendor.tar.gz
  rm -r vendor

  package_source="${maindir}/${PACKAGE}-source-${tag}.tar"
  git archive -o "${package_source}" HEAD
  gzip -f "${package_source}" >"${package_source}.gz"

  cd "${maindir}"

  for i in $sys; do
    os=$(echo $i | cut -f1 -d-)
    arch=$(echo $i | cut -f2 -d-)
    arm=

    if [[ $arch == "armv6" ]]; then
      arch=arm
      arm=6
    elif [[ $arch == "armv7" ]]; then
      arch=arm
      arm=7
    fi

    dir="${PACKAGE}-${i}-${tag}"
    mkdir "${dir}"
    pushd "${dir}"

    green " - Building: ${os} ${arch} ${arm}"
    env CGO_ENABLED=0 GOOS=$os GOARCH=$arch GOARM=$arm go build -v -trimpath -ldflags="${ldflags}" ${PKG}
    popd

    if [[ $os == "windows" ]]; then
      zip -r "${dir}.zip" "${dir}"
    else
      tar -cvzf "${dir}.tar.gz" "${dir}"
    fi

    rm -r "${dir}"
  done

  shasum -a 256 * >manifest-$tag.txt
}

# usage prints the usage of the whole script.
function usage() {
  red "Usage: "
  red "release.sh build-release <version-tag> <build-system(s)> <ldflags>"
}

# Whatever sub command is passed in, we need at least 2 arguments.
if [ "$#" -lt 2 ]; then
  usage
  exit 1
fi

# Extract the sub command and remove it from the list of parameters by shifting
# them to the left.
SUBCOMMAND=$1
shift

# Call the function corresponding to the specified sub command or print the
# usage if the sub command was not found.
case $SUBCOMMAND in
build-release)
  green "Building release"
  build_release "$@"
  ;;
*)
  usage
  exit 1
  ;;
esac
