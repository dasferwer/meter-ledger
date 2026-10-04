#!/bin/sh
set -eu

# Защита охватывает миграции и seed, которые выполняются раньше pytest.
python -m meterledger.test_safety
exec "$@"
