#!/bin/bash
set -e

if [ -d "/app/proto_backup" ]; then
    echo "Syncing generated protos from backup to mounted volume..."

    cp /app/proto_backup/*_pb2*.py /app/db/ 2>/dev/null || true
fi

exec "$@"