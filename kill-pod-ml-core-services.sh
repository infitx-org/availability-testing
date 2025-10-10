#!/usr/bin/env bash
set -euo pipefail
NS="mojaloop"
SLEEP_SECS=120
DRY_RUN="${DRY_RUN:-false}"

# List of deployment names (exact names, not patterns)
deployments=(
  "account-lookup-service"
  "als-msisdn-oracle"
  "centralledger-service"
  "centralledger-handler-transfer-prepare"
  "centralledger-handler-transfer-position"
  "centralledger-handler-transfer-get"
  "centralledger-handler-transfer-fulfil"
  "centralledger-handler-timeout"
  "ml-api-adapter-service"
  "ml-api-adapter-handler-notification"
  "quoting-service"
  "quoting-service-handler"
)

echo "Namespace: ${NS}"
echo "Sleep between components: ${SLEEP_SECS}s"
echo "Dry run: ${DRY_RUN}"
echo

for deployment in "${deployments[@]}"; do
  echo "==> Handling deployment: ${deployment}"

  victim=$(kubectl get pods -n "${NS}" -l "app.kubernetes.io/name=${deployment}" \
    --field-selector=status.phase=Running \
    --no-headers -o custom-columns=":metadata.name" \
    | head -n 1 || true)

  if [[ -z "${victim}" ]]; then
    echo "   No running pods found for deployment '${deployment}'. Skipping."
    echo
    continue
  fi

  echo "   Selected pod to delete: ${victim}"

  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "   [DRY RUN] kubectl delete pod -n ${NS} ${victim}"
  else
    kubectl delete pod -n "${NS}" "${victim}"
  fi

  echo "   Sleeping ${SLEEP_SECS}s before next component..."
  sleep "${SLEEP_SECS}"
  echo
done

echo "All done."