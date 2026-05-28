#!/bin/sh

set -eu

cd "$(dirname "$0")"

REQUIRED_SET_ARGS="--set app.apiKey.hardcoded=01234567890123456789012345678901 --set broker.password.hardcoded=dev-broker-password"
rc=0

for chart_yaml in charts/*/Chart.yaml; do
  if [ ! -f "$chart_yaml" ]; then
    continue
  fi

  chart_dir="$(dirname "$chart_yaml")"
  echo "======= VERIFYING CHART: $chart_dir ======="

  # Ensure local chart dependencies are present if any are declared.
  helm dependency build "$chart_dir" || {
    rc=1
    echo "--------- DEPENDENCY BUILD FAILED: $chart_dir --------"
  }

  helm template "ci-default" "$chart_dir" $REQUIRED_SET_ARGS > /dev/null || {
    rc=1
    echo "--------- TEMPLATE VERIFICATION FAILED: $chart_dir (default values) --------"
  }

  helm lint "$chart_dir" $REQUIRED_SET_ARGS || {
    rc=1
    echo "--------- LINT VERIFICATION FAILED: $chart_dir (default values) --------"
  }

  for values_file in "$chart_dir"/examples/*.yaml; do
    if [ ! -f "$values_file" ]; then
      continue
    fi

    echo "------- VERIFYING EXAMPLE: $values_file --------"

    helm template "ci-example" "$chart_dir" -f "$values_file" > /dev/null || {
      rc=1
      echo "--------- TEMPLATE VERIFICATION FAILED: $values_file --------"
    }

    helm lint "$chart_dir" -f "$values_file" || {
      rc=1
      echo "--------- LINT VERIFICATION FAILED: $values_file --------"
    }

    echo "------- FINISHED VERIFYING EXAMPLE: $values_file --------"
    echo
  done

  echo "======= FINISHED CHART: $chart_dir ======="
  echo
done

exit "$rc"
