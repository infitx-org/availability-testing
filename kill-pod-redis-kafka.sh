#!/usr/bin/env bash
set -euo pipefail

KAFKA_NS="${KAFKA_NS:-moja-kafka}"
REDIS_NS="${REDIS_NS:-mojaloop}"

KAFKA_INTERVAL=300
REDIS_INTERVAL=180
REDIS_OFFSET=90      # start Redis 90s after Kafka
DRY_RUN="${DRY_RUN:-false}"

KAFKA_PODS=(
  "mojaloop-kafka-mojaloop-kafka-nodepool-0"
  "mojaloop-kafka-mojaloop-kafka-nodepool-1"
  "mojaloop-kafka-mojaloop-kafka-nodepool-2"
)
REDIS_PODS=(
  "mojaloop-redis-follower-0"
  "mojaloop-redis-leader-0"
  "mojaloop-redis-follower-1"
  "mojaloop-redis-leader-1"
  "mojaloop-redis-follower-2"
  "mojaloop-redis-leader-2"
)

log(){ echo "[$(date -Is)] $*"; }
delete_pod(){
  ns="$1"; pod="$2"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY RUN] kubectl -n $ns delete pod $pod"
  else
    log "Deleting pod $pod in $ns"
    kubectl -n "$ns" delete pod "$pod" || true
  fi
}

k=0; r=0
t_now=0
t_kafka=0
t_redis=$REDIS_OFFSET

log "Start | Kafka every ${KAFKA_INTERVAL}s | Redis every ${REDIS_INTERVAL}s (offset ${REDIS_OFFSET}s) | dry_run=$DRY_RUN"

while (( k < ${#KAFKA_PODS[@]} || r < ${#REDIS_PODS[@]} )); do
  next=$((1<<30))
  if (( k < ${#KAFKA_PODS[@]} )) && (( t_kafka < next )); then
    next=$t_kafka
  fi
  if (( r < ${#REDIS_PODS[@]} )) && (( t_redis < next )); then
    next=$t_redis
  fi

  sleep_secs=$(( next - t_now ))
  if (( sleep_secs > 0 )); then
    sleep "$sleep_secs"
  fi
  t_now=$next

  if (( k < ${#KAFKA_PODS[@]} )) && (( t_now == t_kafka )); then
    delete_pod "$KAFKA_NS" "${KAFKA_PODS[$k]}"
    ((k+=1))
    t_kafka=$(( t_kafka + KAFKA_INTERVAL ))
  fi

  if (( r < ${#REDIS_PODS[@]} )) && (( t_now == t_redis )); then
    delete_pod "$REDIS_NS" "${REDIS_PODS[$r]}"
    ((r+=1))
    t_redis=$(( t_redis + REDIS_INTERVAL ))
  fi
done

log "All done."
