#!/usr/bin/env bash
#
# View events published by the outbox relay to Floci's SNS -> SQS fan-out.
#
# The relay publishes outbox events to the SNS topic `payment-events`, which
# fans out to the `payment-events-consumer` SQS queue (created by floci-init).
# This script reads that queue using the amazon/aws-cli image, so no AWS CLI
# is needed on the host.
#
# Usage:
#   ./scripts/sqs-tail.sh peek   # one-shot: receive and print up to 10 messages (non-destructive)
#   ./scripts/sqs-tail.sh tail   # poll every 2s, print each message, then delete it
#
# Env overrides (defaults match the docker-compose Floci stack / .env):
#   SQS_QUEUE_URL   default http://localhost:4566/000000000000/payment-events-consumer
#   AWS_ENDPOINT_URL  default http://localhost:4566
#   AWS_DEFAULT_REGION default us-east-1
#   AWS_ACCESS_KEY_ID  default test
#   AWS_SECRET_ACCESS_KEY default test

set -euo pipefail

QUEUE_URL="${SQS_QUEUE_URL:-http://localhost:4566/000000000000/payment-events-consumer}"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
AKID="${AWS_ACCESS_KEY_ID:-test}"
SECRET="${AWS_SECRET_ACCESS_KEY:-test}"

cmd="${1:-peek}"

aws() {
  docker run --rm --network host \
    -e AWS_ACCESS_KEY_ID="$AKID" \
    -e AWS_SECRET_ACCESS_KEY="$SECRET" \
    -e AWS_DEFAULT_REGION="$REGION" \
    amazon/aws-cli:latest "$@"
}

receive() {
  aws sqs receive-message \
    --queue-url "$QUEUE_URL" \
    --endpoint-url "$ENDPOINT" \
    --max-number-of-messages 10 \
    --attribute-names All \
    --message-attribute-names All
}

delete() {
  local handle="$1"
  aws sqs delete-message \
    --queue-url "$QUEUE_URL" \
    --endpoint-url "$ENDPOINT" \
    --receipt-handle "$handle"
}

case "$cmd" in
  peek)
    receive
    ;;
  tail)
    while :; do
      out=$(receive)
      if [ -z "$out" ] || [ "$out" = '{"Messages": []}' ]; then
        sleep 2
        continue
      fi
      if ! command -v jq >/dev/null 2>&1; then
        echo "$out"
        echo "--- (install jq to auto-delete; messages remain in flight until visibility timeout) ---"
        sleep 2
        continue
      fi
      count=$(printf '%s' "$out" | jq '.Messages | length')
      for i in $(seq 0 $((count - 1))); do
        printf '%s' "$out" | jq -c ".Messages[$i] | {body: .Body, attributes: .Attributes, message_attributes: .MessageAttributes}"
        handle=$(printf '%s' "$out" | jq -r ".Messages[$i].ReceiptHandle")
        delete "$handle"
      done
      echo "---"
    done
    ;;
  *)
    echo "usage: $0 {peek|tail}" >&2
    exit 1
    ;;
esac
