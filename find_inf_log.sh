#!/bin/sh

rsync -avz -e ssh od99usr@:/informatica/infa/logs/node01/services/DataIntegrationService/disLogs/workflow/*$1* $2
# tar -zcvf $2.tar.gz $2
 