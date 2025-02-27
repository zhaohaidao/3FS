#!/bin/bash

RED='\033[0;31m'
NC='\033[0m' # No Color

function check_exists {
    if ! [ -e $1 ]; then
        echo $1 not exists
        exit 1
    fi
}

function random_port {
    local min_port=8000
    local max_port=65535

    while true; do
        random_port=$((min_port + RANDOM % (max_port - min_port + 1)))
        if ! lsof -i ":$random_port" >/dev/null; then
            echo "$random_port"
            break
        fi
    done
}

function start_fdb {
    local PORT=$1
    local DATA=$2
    local LOG=$3
    local CLUSTER=${DATA}/fdb.cluster

    echo fdbserver: $FDB
    echo fdbclient: $FDBCLI
    echo start fdb @ port ${PORT}
    echo test${FDB_PORT}:testdb${FDB_PORT}@127.0.0.1:${FDB_PORT} >${CLUSTER}
    ${FDB} -p auto:${PORT} -d ${DATA} -L ${LOG} -C ${CLUSTER} &> ${LOG}/fdbserver.log &

    echo "sleep 5s then create database"
    sleep 5 && $FDBCLI -C ${CLUSTER} --exec "configure new memory single"
    echo "sleep 5s then check foundationdb status"
    sleep 5 && $FDBCLI -C ${CLUSTER} --exec "status minimal"
}

function create_app_config {
    local NODE=$1
    local FILE=$2

    echo "allow_empty_node_id = false" >$FILE
    echo "node_id = $NODE" >>$FILE
}

function wait_path {
    local file_path="$1"
    local timeout=$2
    local interval=1  # Check interval in seconds
    local elapsed_time=0

    # Wait until the file exists or timeout is reached
    while [ ! -d "$file_path" ] && [ $elapsed_time -lt $timeout ]; do
        sleep $interval
        elapsed_time=$((elapsed_time + interval))
    done
    
    if ! [ -d "$file_path" ]; then
        echo "Path '$file_path' not found within $timeout seconds."
        exit 1
    fi
}

function random_64 {
    local random_number=$(od -An -N8 -tu8 /dev/urandom | awk '{print $1}')
    echo "$random_number"
}

if (($# < 2)); then
    echo "${0} <binary> <test-dir> <log-dir>"
    exit 1
fi

BINARY=${1%/}
TEST_DIR=${2%/}
LOG_DIR=${3:-}

function cleanup {
    rv=$?
    echo exit with ${rv}
    fusermount3 -u ${MOUNT1} || true
    fusermount3 -u ${MOUNT2} || true
    fusermount3 -u ${MOUNT3} || true
    fusermount3 -u ${MOUNT4} || true
    sleep 1
    jobs -p | xargs -r kill || true
    sleep 1
    jobs -p | xargs -r kill -9 &> /dev/null || true
    fusermount3 -u ${MOUNT1} &> /dev/null || true
    fusermount3 -u ${MOUNT2} &> /dev/null || true
    fusermount3 -u ${MOUNT3} &> /dev/null || true
    fusermount3 -u ${MOUNT4} &> /dev/null || true
    exit $rv
}

trap "cleanup" EXIT
set -euo pipefail

DATA=${TEST_DIR}/data
CONFIG=${TEST_DIR}/config
DEFAULT_LOG=${TEST_DIR}/log
LOG=${LOG_DIR:=${DEFAULT_LOG}}
MOUNT1=`realpath ${TEST_DIR}/mnt1`
MOUNT2=`realpath ${TEST_DIR}/mnt2`
MOUNT3=`realpath ${TEST_DIR}/mnt3`
MOUNT4=`realpath ${TEST_DIR}/mnt4`

export FUSE_TEST_TIMEOUT="${FUSE_TEST_TIMEOUT:-3600}"

SCRIPT_DIR=$(dirname "$0")
UPDATE_CONFIG="python3 ${SCRIPT_DIR}/update-config.py"

# NOTE: need regenerate chain table if change storage number or disk number
NUM_META=2
NUM_STORAGE=3
NUM_DISK=4
export TOKEN="AADHHSOs8QA92iRe2wB1fmuL"
export TOKEN_FILE="${CONFIG}/token"
export PKEY_INDEX=${PKEY_INDEX:-0}
export FS_CLUSTER="ci_test"
export FDB_UNITTEST_CLUSTER=${DATA}/foundationdb/fdb.cluster
export TARGETS="$(seq -f \'{{\ env.STORAGE_DIR\ }}/data%g\', 1 ${NUM_DISK})"

if [ -z "${FDB_PATH+x}" ]; then
    FDB=$(which fdbserver)
    FDBCLI=$(which fdbcli)
else
    FDB=$FDB_PATH/fdbserver
    FDBCLI=$FDB_PATH/fdbcli
fi

# check network
echo "### check network"
export ADDRESS=$(ip -o -4 addr show | grep -E '(en|eth|ib)' | awk '{print $4}' | head -n 1 | awk -F'[/ ]' '{print $1}')
export FDB_PORT=$(random_port)
export MGMTD_PORT=$(random_port)
if [ -n "${FUSE_TEST_FDB_PORT+x}" ]; then
    echo use FUSE_TEST_FDB_PORT ${FUSE_TEST_FDB_PORT}
    lsof -i ":${FUSE_TEST_FDB_PORT}" || echo port available
    export FDB_PORT=${FUSE_TEST_FDB_PORT}
fi
if [ -n "${FUSE_TEST_MGMTD_PORT+x}" ]; then
    echo use FUSE_TEST_MGMTD_PORT ${FUSE_TEST_MGMTD_PORT}
    lsof -i ":${FUSE_TEST_MGMTD_PORT}" || echo port available
    export MGMTD_PORT=${FUSE_TEST_MGMTD_PORT}
fi
echo "NIC address ${ADDRESS}"
echo "FDB port ${FDB_PORT}"
echo "MGMTD port ${MGMTD_PORT}"

# check directory
echo "### check directory and file"
check_exists ${TEST_DIR}
rm -rf ${TEST_DIR}/*
check_exists ${BINARY}
check_exists ${BINARY}/mgmtd_main
check_exists ${BINARY}/meta_main
check_exists ${BINARY}/storage_main
check_exists ${BINARY}/hf3fs_fuse_main
check_exists ${BINARY}/admin_cli
check_exists ${FDB}
check_exists ${FDBCLI}

# create subdirectory
echo "### create subdirectory"
mkdir -p ${CONFIG}
mkdir -p ${LOG}
mkdir -p ${DATA}
mkdir -p ${MOUNT1}
mkdir -p ${MOUNT2}
mkdir -p ${MOUNT3}
mkdir -p ${MOUNT4}
mkdir -p ${DATA}/foundationdb
for i in $(seq 1 ${NUM_STORAGE}); do
    mkdir -p ${DATA}/storage-${i}
    for j in $(seq 1 ${NUM_DISK}); do
        mkdir -p ${DATA}/storage-${i}/data${j}
    done
done

# generate app config
echo "generate app config"
create_app_config 1 ${CONFIG}/mgmtd_main_app.toml
for i in $(seq 1 ${NUM_META}); do
    node=$(expr 50 + ${i})
    file=${CONFIG}/meta_main_app_${node}.toml
    create_app_config ${node} ${file}
done
create_app_config 51 ${CONFIG}/meta_main_app.toml
for i in $(seq 1 ${NUM_STORAGE}); do
    node=$(expr 10000 + ${i})
    file=${CONFIG}/storage_main_app_${node}.toml
    create_app_config ${node} ${file}
done

# generate config
echo "generate config"
echo ${TOKEN} > ${TOKEN_FILE}
for file in ${SCRIPT_DIR}/config/*.toml; do
    echo "- generate $(basename $file)"
    cat ${file} | envsubst > ${CONFIG}/$(basename $file)
done
ADMIN_CLI="${BINARY}/admin_cli --cfg ${CONFIG}/admin_cli.toml --"

# start foundationdb
echo "### create foundationdb"
start_fdb $FDB_PORT ${DATA}/foundationdb ${LOG}/

# init cluster
echo "### init cluster"
echo "admin_cli: ${ADMIN_CLI}"
${ADMIN_CLI} user-add --root --admin --token ${TOKEN} 0 root
${ADMIN_CLI} user-set-token --new 0

echo ""
${ADMIN_CLI} user-list
echo ""
${ADMIN_CLI} init-cluster \
    --mgmtd ${CONFIG}/mgmtd_main.toml \
    --meta ${CONFIG}/meta_main.toml \
    --storage ${CONFIG}/storage_main.toml \
    --fuse ${CONFIG}/hf3fs_fuse_main.toml \
    --skip-config-check 1 524288 8

# start mgmtd
echo "### start mgmtd"
LOG_FILE=${LOG}/mgmtd.log \
    PORT=${MGMTD_PORT} \
    ${BINARY}/mgmtd_main \
    --app_cfg ${CONFIG}/mgmtd_main_app.toml \
    --launcher_cfg ${CONFIG}/mgmtd_main_launcher.toml \
    --cfg ${CONFIG}/mgmtd_main.toml \
    >${LOG}/mgmtd_main.stdout 2>${LOG}/mgmtd_main.stderr &
sleep 5 && echo ""

# start meta
echo "### start meta & storage"
for i in $(seq 1 ${NUM_STORAGE}); do
    LOG_FILE=${LOG}/storage-${i}.log \
        PORT=0 \
        STORAGE_DIR=${DATA}/storage-${i}/ \
        ${BINARY}/storage_main \
        --app_cfg ${CONFIG}/storage_main_app_$(expr 10000 + ${i}).toml \
        --launcher_cfg ${CONFIG}/storage_main_launcher.toml \
        --cfg ${CONFIG}/storage_main.toml \
        >${LOG}/storage_main-${i}.stdout 2>${LOG}/storage_main-${i}.stderr &
done

for i in $(seq 1 ${NUM_META}); do
    LOG_FILE=${LOG}/meta-${i}.log \
        PORT=0 \
        ${BINARY}/meta_main \
        --app_cfg ${CONFIG}/meta_main_app_$(expr 50 + ${i}).toml \
        --launcher_cfg ${CONFIG}/meta_main_launcher.toml \
        --cfg ${CONFIG}/meta_main.toml \
        >${LOG}/meta_main-${i}.stdout 2>${LOG}/meta_main-${i}.stderr &
done

sleep 15 && echo ""

# list nodes
echo "### list nodes"
${ADMIN_CLI} list-nodes

# create chains
echo "### create targets and chain table"
echo "create targets"
echo "ChainId,TargetId" >${CONFIG}/chains.csv
echo "ChainId" >${CONFIG}/chain-table.csv
for storage in $(seq 1 ${NUM_STORAGE}); do
    node=$(expr 10000 + ${storage})
    for disk in $(seq 1 ${NUM_DISK}); do
        target=$(printf "%d%02d001" $node $disk)
        args="create-target --node-id ${node} --disk-index $((disk - 1)) --target-id ${target} --chain-id ${target}"
        echo $args
        echo "${target},${target}" >>${CONFIG}/chains.csv
        echo "${target}" >>${CONFIG}/chain-table.csv
        ${ADMIN_CLI} ${args}
    done
done

echo "update chains and chain table"
${ADMIN_CLI} upload-chains ${CONFIG}/chains.csv
${ADMIN_CLI} upload-chain-table 1 ${CONFIG}/chain-table.csv --desc replica-1
sleep 10
${ADMIN_CLI} list-chains
${ADMIN_CLI} list-chain-tables

echo "create test user"
if [ $UID -ne 0 ]; then
    ${ADMIN_CLI} user-add ${UID} test
fi
${ADMIN_CLI} user-list
${ADMIN_CLI} mkdir --perm 0755 test
${ADMIN_CLI} set-perm --uid ${UID} --gid ${UID} test

echo "### start fuse"
echo "redirect stdout to ${LOG}/fuse.1.stdout, stderr to ${LOG}/fuse.1.stderr"
LOG_FILE=${LOG}/fuse.1.log DYNAMIC_STRIPE=true READCACHE=false \
    timeout -k 10 ${FUSE_TEST_TIMEOUT} ${BINARY}/hf3fs_fuse_main \
    --launcher_cfg ${CONFIG}/hf3fs_fuse_main_launcher.toml \
    --launcher_config.mountpoint=${MOUNT1} \
    >${LOG}/fuse.1.stdout 2>${LOG}/fuse.1.stderr &

echo "redirect stdout to ${LOG}/fuse.2.stdout, stderr to ${LOG}/fuse.2.stderr"
LOG_FILE=${LOG}/fuse.2.log ENABLE_WRITE_BUFFER=1 DYNAMIC_STRIPE=false READCACHE=false \
    timeout -k 10 ${FUSE_TEST_TIMEOUT} ${BINARY}/hf3fs_fuse_main \
    --launcher_cfg ${CONFIG}/hf3fs_fuse_main_launcher.toml \
    --launcher_config.mountpoint=${MOUNT2} \
    >${LOG}/fuse.2.stdout 2>${LOG}/fuse.2.stderr &

echo "redirect stdout to ${LOG}/fuse.3.stdout, stderr to ${LOG}/fuse.3.stderr"
LOG_FILE=${LOG}/fuse.3.log DYNAMIC_STRIPE=false READCACHE=true \
    timeout -k 10 ${FUSE_TEST_TIMEOUT} ${BINARY}/hf3fs_fuse_main \
    --launcher_cfg ${CONFIG}/hf3fs_fuse_main_launcher.toml \
    --launcher_config.mountpoint=${MOUNT3} \
    >${LOG}/fuse.3.stdout 2>${LOG}/fuse.3.stderr &

echo "redirect stdout to ${LOG}/fuse.4.stdout, stderr to ${LOG}/fuse.4.stderr"
LOG_FILE=${LOG}/fuse.4.log ENABLE_WRITE_BUFFER=1 DYNAMIC_STRIPE=true READCACHE=true \
    timeout -k 10 ${FUSE_TEST_TIMEOUT} ${BINARY}/hf3fs_fuse_main \
    --launcher_cfg ${CONFIG}/hf3fs_fuse_main_launcher.toml \
    --launcher_config.mountpoint=${MOUNT4} \
    >${LOG}/fuse.4.stdout 2>${LOG}/fuse.4.stderr &

wait_path ${MOUNT1}/test 30
wait_path ${MOUNT2}/test 30
wait_path ${MOUNT3}/test 30
wait_path ${MOUNT4}/test 30

if [ -z "${FIO_BINARY+x}" ]; then
    if [ -f ${BINARY}/fio ]; then
        FIO_BINARY=${BINARY}/fio
    else
        FIO_BINARY=""
    fi
fi

echo "Fuse mounted"

python3 ${SCRIPT_DIR}/concurrent_rmrf.py --mnt_path ${MOUNT1}

echo "### test gc permission check"
mkdir -p ${MOUNT1}/test/data/subdir{1..1024}
sudo mkdir ${MOUNT1}/test/data/subdir999/root
sudo touch ${MOUNT1}/test/data/subdir999/root/file-{1..5}
mv ${MOUNT1}/test/data ${MOUNT1}/3fs-virt/rm-rf
sleep 5
stat ${MOUNT1}/trash/gc-orphans
ls -lR ${MOUNT1}/trash/gc-orphans

echo "### test trash"
bash ${SCRIPT_DIR}/test_trash.sh ${BINARY} ${TEST_DIR}

echo "### test ioctl"
sudo rm -rf ${MOUNT1}/trash/`id -un`/*
python3 ${SCRIPT_DIR}/test_ioctl.py ${MOUNT1} ${MOUNT1}/test/ioctl

for MOUNT in ${MOUNT1} ${MOUNT2} ${MOUNT3} ${MOUNT4}; do
    new_chunk_engine=$((( RANDOM % 2 == 0 )) && echo "true" || echo "false")
    echo "### set new_chunk_engine to ${new_chunk_engine}"
    ${ADMIN_CLI} hot-update-config --node-type META --string "server.meta.enable_new_chunk_engine=${new_chunk_engine}"

    echo "### run test under ${MOUNT}"
    
    SUBDIR=${MOUNT}/test/$(random_64)
    mkdir -p ${SUBDIR}
    echo "use tmp dir ${SUBDIR}"

    echo "#### run ci test"
    mkdir ${SUBDIR}/fuse_test_ci
    python3 ${SCRIPT_DIR}/fuse_test_ci.py ${SUBDIR}/fuse_test_ci
    echo ""

    echo "#### test read after write"
    mkdir ${SUBDIR}/read_after_write
    python3 ${SCRIPT_DIR}/read_after_write.py ${SUBDIR}/read_after_write
    echo ""

    echo "### test client io during update chain table"
    # start a read/write task
    mkdir ${SUBDIR}/rw_during_update_chain_table
    python3 ${SCRIPT_DIR}/read_after_write.py ${SUBDIR}/rw_during_update_chain_table --seconds 60 &
    PID=$!
    # then update chain table in background
    sleep 10
    start_time=$(date +%s)
    while true; do
        current_time=$(date +%s)
        elapsed=$(( current_time - start_time ))
        if [ $elapsed -lt 30 ]; then
            echo "update chain table"
            (head -n 1 ${CONFIG}/chain-table.csv && tail -n +2 ${CONFIG}/chain-table.csv | shuf) > ${CONFIG}/chain-table-shuffle.csv
            ${ADMIN_CLI} upload-chain-table 1 ${CONFIG}/chain-table-shuffle.csv --desc replica-1-shuflle
            ${ADMIN_CLI} list-chain-tables
        else
            break
        fi
        sleep 10
    done
    wait $PID
    echo ""

    echo "#### test random write and read"
    mkdir ${SUBDIR}/random_rw
    python3 ${SCRIPT_DIR}/random_rw.py ${SUBDIR}/random_rw
    echo ""

    echo "#### test read before close"
    mkdir ${SUBDIR}/read_before_close
    python3 ${SCRIPT_DIR}/random_rw.py --read_before_close ${SUBDIR}/read_before_close
    echo ""

    set +e
    python3 -c 'import hf3fs_fuse.io as h3io'
    ret=$?
    set -e
    if [ ${ret} -eq 0 ]; then
        # cat /proc/self/mountinfo
        # echo
        # grep hf3fs /proc/self/mountinfo | awk '{print $5, $(NF-2)}'
        # ls $(grep hf3fs /proc/self/mountinfo | awk '{print $5}')/
        # ls ${MOUNT}
        # echo ${SUBDIR}/usrbio
        echo '#### test usrbio'
        mkdir ${SUBDIR}/usrbio
        python3 ${SCRIPT_DIR}/usrbio.py ${SUBDIR}/usrbio ${MOUNT}
    else
        echo -e "${RED}#### can't inmport hf3fs_fuse.io, skip usrbio test!!!${NC}"
    fi

    if [ -f "${FIO_BINARY}" ]; then
        echo "#### test fio with usrbio"
        mkdir ${SUBDIR}/fio
        for j in {0..3}; do
            for f in {0..2}; do
                truncate -s 4G ${SUBDIR}/fio/j${j}-f${f}
            done
        done
        date
        LD_PRELOAD="${BINARY}/libhf3fs_api_shared.so" ${FIO_BINARY} -name=test_write -rw=write -direct=1 -ioengine=hf3fs_ring -group_reporting -numjobs=4 -bs=4m -size=4G -directory=${SUBDIR}/fio -filename_format='j$jobnum-f$filenum' -thread=1 -mountpoint=${MOUNT} -verify=crc32c -nrfiles=2
        date
        ls -lh ${SUBDIR}/fio
        date
        LD_PRELOAD="${BINARY}/libhf3fs_api_shared.so" ${FIO_BINARY} -name=test_read -rw=randread -direct=1 -ioengine=hf3fs_ring -group_reporting -numjobs=4 -bs=4k -iodepth=100 -time_based -runtime=30 -directory=${SUBDIR}/fio -filename_format='j$jobnum-f$filenum' -thread=1 -mountpoint=${MOUNT} -iodepth_batch_submit=100 -iodepth_batch_complete_min=100 -iodepth_batch_complete_max=100 -size=4G -nrfiles=2
        date
    else
        echo "${RED}#### fio not found, skip fio test!!!${NC}"
    fi

    if [ -f ${BINARY}/pjdfstest ]; then
        echo "#### run pjdfstest"
        ${BINARY}/pjdfstest -c ${CONFIG}/pjdfstest.toml -p ${SUBDIR}
    else
        echo "${RED}#### pjdfstest not found, skip pjdfstest!!!${NC}"
    fi

    touch ${SUBDIR}/test_iflags
    python3 ${SCRIPT_DIR}/test_iflags.py ${SUBDIR}/test_iflags
done

echo "### test concurrent rw"
mkdir -p ${MOUNT1}/test/concurrent_rw1 ${MOUNT2}/test/concurrent_rw1
python3 ${SCRIPT_DIR}/concurrent_rw.py ${MOUNT1}/test/concurrent_rw1 ${MOUNT2}/test/concurrent_rw1

mkdir -p ${MOUNT1}/test/concurrent_rw2 ${MOUNT2}/test/concurrent_rw2
python3 ${SCRIPT_DIR}/concurrent_rw.py ${MOUNT2}/test/concurrent_rw2 ${MOUNT1}/test/concurrent_rw2

echo "### test lock directory"
mkdir -p ${MOUNT1}/test/test_dir_lock
python3 ${SCRIPT_DIR}/lock_directory.py ${MOUNT1}/test/test_dir_lock ${MOUNT2}/test/test_dir_lock

echo "### test client io during update chain table"
(head -n 1 ${CONFIG}/chain-table.csv && tail -n +2 ${CONFIG}/chain-table.csv | shuf) > ${CONFIG}/chain-table-2.csv
${ADMIN_CLI} upload-chain-table 1 ${CONFIG}/chain-table-2.csv --desc replica-1-version-2
${ADMIN_CLI} list-chain-tables

echo "### test chattr +i"
sudo chattr +i ${MOUNT1}/test
lsattr ${MOUNT1}
sudo chattr -i ${MOUNT1}/test

echo "unmount fuse and stop all servers"
fusermount3 -u ${MOUNT1}
fusermount3 -u ${MOUNT2}
fusermount3 -u ${MOUNT3}
fusermount3 -u ${MOUNT4}

# 定义函数 kill_and_wait
kill_and_wait() {
    local pid="$1"
    local cmd_name="$2"
    # 检查进程是否存在
    if ps -p "$pid" > /dev/null; then
        # 杀死进程
        kill "$pid"
        echo "Sent kill signal to process $cmd_name with PID $pid"
        # 等待并检查进程是否退出
        for i in {1..5}; do
        sleep 1
        if ! ps -p "$pid" > /dev/null; then
            echo "Process $cmd_name with PID $pid has exited."
            return 0
        fi
        done
        # 如果5秒后进程仍然存在
        echo -e "\e[31mError: Process $cmd_name with PID $pid did not exit after 5 seconds.\e[0m"
        kill -9 "$pid"
    else
        echo "No process with PID $pid found."
        return 0
    fi
}

# 获取所有后台任务的PID和命令名
jobs_array=()
while read -r pid cmd; do
    jobs_array+=("$pid $cmd") 
done < <(jobs -l | awk '{print $2, $3, $4, $5, $6, $7, $8, $9, $10}')

# 按照启动顺序的相反顺序进行 kill
for (( idx=${#jobs_array[@]}-1 ; idx>=0 ; idx-- )); do
    job_info=(${jobs_array[idx]})
    pid=${job_info[0]}
    cmd_name=${job_info[@]:1}
    kill_and_wait "$pid" "$cmd_name"
done