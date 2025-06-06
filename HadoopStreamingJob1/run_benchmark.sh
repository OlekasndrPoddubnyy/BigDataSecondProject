#!/bin/bash

# Percorsi
MAPPER=mapper.py
REDUCER=reducer.py
INPUT_DIR=input
OUTPUT_DIR=output
LOG_FILE=benchmark_times.txt

# Reset log
echo "Benchmark MapReduce" > $LOG_FILE

# Liste dimensioni
for SIZE in 100k 500k 1M full; do
    INPUT_FILE="$INPUT_DIR/used_${SIZE}.csv"
    OUTPUT_PATH="$OUTPUT_DIR/$SIZE"

    echo "▶️ Esecuzione per input: $SIZE"

    # Rimuovi output precedente
    rm -rf "$OUTPUT_PATH"

    # Misura tempo
    START=$(date +%s)

    HADOOP_STREAMING_JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar)

    hadoop jar $HADOOP_STREAMING_JAR \
        -file $MAPPER -mapper "python3 $MAPPER" \
        -file $REDUCER -reducer "python3 $REDUCER" \
        -input "$INPUT_FILE" \
        -output "$OUTPUT_PATH"

    END=$(date +%s)
    DURATION=$((END - START))

    echo "✅ $SIZE completato in $DURATION secondi"
    echo "$SIZE: $DURATION sec" >> $LOG_FILE
done