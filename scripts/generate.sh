#!/bin/bash

PROJECT="polar-scene-465223-f7"
REGION="europe-west1"

# Récupérer tous les jobs Garmin
JOBS=$(gcloud run jobs list --region=$REGION \
  --filter="metadata.name~garmin" \
  --format="value(metadata.name)" \
  | sort)

echo "main:"
echo "  steps:"
echo "    - log_start:"
echo "        call: sys.log"
echo "        args:"
echo "          text: \"⌚ Garmin daily workflow - 28 endpoints (3 days history)\""
echo "          severity: INFO"
echo ""
echo "    - parallel_fetch_garmin:"
echo "        parallel:"
echo "          branches:"

# Générer les branches
for job in $JOBS; do
  # Extraire le nom court (après "garmin-")
  short_name=${job#garmin-}
  branch_name="fetch_${short_name//-/_}"
  
  echo "            - $branch_name:"
  echo "                steps:"
  echo "                  - call_job:"
  echo "                      call: googleapis.run.v2.projects.locations.jobs.run"
  echo "                      args:"
  echo "                        name: projects/$PROJECT/locations/$REGION/jobs/$job"
  echo ""
done

echo "    - log_fetch_complete:"
echo "        call: sys.log"
echo "        args:"
echo "          text: \"✅ All Garmin fetchers completed (parallel)\""
echo "          severity: INFO"
echo ""
echo "    - ingest_garmin:"
echo "        call: googleapis.run.v2.projects.locations.jobs.run"
echo "        args:"
echo "          name: projects/$PROJECT/locations/$REGION/jobs/garmin-ingest"
echo "        result: ingest_result"
echo ""
echo "    - log_ingest_complete:"
echo "        call: sys.log"
echo "        args:"
echo "          text: \${\"✅ Garmin ingest completed: \" + ingest_result.name}"
echo "          severity: INFO"
echo ""
echo "    - run_dbt:"
echo "        call: googleapis.run.v2.projects.locations.jobs.run"
echo "        args:"
echo "          name: projects/$PROJECT/locations/$REGION/jobs/dbt-run-garmin"
echo "        result: dbt_result"
echo ""
echo "    - log_dbt_complete:"
echo "        call: sys.log"
echo "        args:"
echo "          text: \${\"✅ dbt Garmin completed: \" + dbt_result.name}"
echo "          severity: INFO"
echo ""
echo "    - return_success:"
echo "        return:"
echo "          status: \"SUCCESS\""
echo "          message: \"⌚ Garmin daily workflow completed\""
echo "          timestamp: \${sys.now()}"
echo "          executions:"
echo "            ingest: \${ingest_result.name}"
echo "            dbt: \${dbt_result.name}"