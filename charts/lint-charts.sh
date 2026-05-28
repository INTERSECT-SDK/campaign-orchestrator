#!/bin/sh

set -eu

cd "$(dirname "$0")"

rc=0

for chart_yaml in */Chart.yaml; do
	chart_dir="$(dirname "$chart_yaml")"
	echo "======= VERIFYING CHART: $chart_dir ======="

	# Ensure local chart dependencies are present if any are declared.
	helm dependency update "$chart_dir" || {
		rc=1
		echo "--------- DEPENDENCY BUILD FAILED: $chart_dir --------"
	}

	for values_file in "$chart_dir"/examples/*.yaml; do
		echo "------- VERIFYING EXAMPLE: $values_file --------"

		helm template "$chart_dir" -f "$values_file" >/dev/null || {
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
