#!/bin/bash

cd $(dirname $0)/..

EXEC="docker build"

USER="zw"
TAG="V5.0"

# 确保进入 DeathStarBench 目录
cd ../
ROOT_FOLDER=$(pwd)

# # 用 docker 驱动新建 builder，保证能复用 daemon 本地镜像
# $EXEC create --name default --use --driver docker 2>/dev/null || true

for i in hotelreservation
do
  IMAGE=${i}
  echo Processing image ${IMAGE}
  cd $ROOT_FOLDER
  # 注意不要加 --pull
  $EXEC -t "$USER"/"$IMAGE":"$TAG" -f Dockerfile . --platform linux/amd64 --load
  cd $ROOT_FOLDER
  echo
done

cd - >/dev/null