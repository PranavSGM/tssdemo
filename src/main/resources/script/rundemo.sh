#!/usr/bin/env bash
sh ~/workspace/tssdemo/src/main/resources/script/emptydir.sh
sh ~/workspace/tssdemo/src/main/resources/script/createdir.sh
sh ~/workspace/tssdemo/src/main/resources/script/ingestion.sh
#sh ~/workspace/tssdemo/src/main/resources/script/startservices.sh
sh ~/workspace/tssdemo/src/main/resources/script/transform.sh
sh ~/workspace/tssdemo/src/main/resources/script/impalaquery.sh

