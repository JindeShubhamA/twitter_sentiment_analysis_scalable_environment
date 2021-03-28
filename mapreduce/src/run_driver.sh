#!/bin/bash

export PROJECT_ROOT=""
export SPARK_HOME="$(cat /SPARK_HOME)"

echo "spark home is set to: $SPARK_HOME"

# start the history server to keep spark ui accessible even after the job finishes
"$SPARK_HOME/sbin/start-history-server.sh" --properties-file "$PROJECT_ROOT/spark-configs/spark-history-server.conf"
# start the python driver
python "main.py"

echo "Spark Driver finished, keeping Spark history server open"
# sleep forever after the driver finishes, so the history server can stay active
sh -c "trap : TERM INT; sleep 9999999999d & wait"