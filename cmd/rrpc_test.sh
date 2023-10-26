#!/bin/bash

user="wq"
passwd="wq"
CMD_DIR="/home/$user/hot-t/build/test"
SUDO="echo $passwd | sudo -S"

SERVER=44
CLIENT=33
SIZE=64

kill_all() {
  echo "kill all"
  sshpass -p $passwd ssh $user@192.168.1.$SERVER "$SUDO killall rrpc_coroutine_test" &
  sshpass -p $passwd ssh $user@192.168.1.$CLIENT "$SUDO killall rrpc_coroutine_test" &
}

test_run() {
  kill_all
  # scp
  echo "[prepare] scp $CMD_DIR/rrpc_coroutine_test to $user@192.168.1.$CLIENT"
  echo "[prepare] scp $CMD_DIR/rrpc_coroutine_test to $user@192.168.1.$SERVER"

  sshpass -p $passwd scp $CMD_DIR/rrpc_coroutine_test $user@192.168.1.$CLIENT:/home/wq
  sshpass -p $passwd scp $CMD_DIR/rrpc_coroutine_test $user@192.168.1.$SERVER:/home/wq

  SERVER_TEST="$SUDO ./rrpc_coroutine_test s 192.168.1.$SERVER 4 $SIZE >> result_rrpc_coroutine_test.log 2>&1"

  # 启动server
  echo "[exec] $SERVER_TEST"
  sshpass -p $passwd ssh $user@192.168.1.$SERVER $SERVER_TEST &

  echo "[exec] $SUDO ./$*"
  sshpass -p $passwd ssh $user@192.168.1.$CLIENT "$SUDO ./$*"

  # echo "[exec] $SUDO $CMD_DIR/$*  >> result_test.log 2>&1"
  # echo $passwd | sudo -S $CMD_DIR/$* >> result_test.log 2>&1
  return $?
  # 关闭master、dn_server进程
}

test_retry() {
  while
    echo $*
    test_run $*
    [ $? != 0 ]
  do
    :
  done
}

echo "Start ..."

test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 64 1000000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 2 64 1000000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 4 64 1000000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 8 64 1000000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 16 64 1000000 4 10 2 8 16

SIZE=512
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 512 400000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 2 512 400000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 4 512 400000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 8 512 400000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 16 512 400000 4 10 2 8 16

SIZE=4096
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 4096 100000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 2 4096 100000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 4 4096 100000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 8 4096 100000 4 10 2 8 16
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 16 4096 100000 4 10 2 8 16

SIZE=64
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 64 100000 1 1 1 1 1
SIZE=128
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 128 100000 1 1 1 1 1
SIZE=256
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 256 100000 1 1 1 1 1
SIZE=512
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 512 100000 1 1 1 1 1
SIZE=1024
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 1024 100000 1 1 1 1 1
SIZE=2048
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 2048 100000 1 1 1 1 1
SIZE=4096
test_retry rrpc_coroutine_test c 192.168.1.$SERVER 1 4096 100000 1 1 1 1 1