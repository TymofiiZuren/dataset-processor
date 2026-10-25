#!/bin/sh
# Default tests own a separate database; never migrate the running workspace.
# Explicit service endpoints must be reachable; unavailable services fail.
set -eu
if [ -z "${PROCESSOR_TEST_DATABASE_URL:-}" ]; then
  docker compose -f compose.local.yaml exec -T postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 <<'SQL'
SELECT 'CREATE DATABASE processor_aws_test'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='processor_aws_test')\gexec
SQL
  export PROCESSOR_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:15439/processor_aws_test?sslmode=disable'
fi
export PROCESSOR_TEST_ENDPOINT="${PROCESSOR_TEST_ENDPOINT:-http://127.0.0.1:15019}"
go test -race ./... -count=1
