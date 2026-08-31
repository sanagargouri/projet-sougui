#!/bin/bash

# Trouve l'IP Windows qui répond sur le port 1433
echo "🔍 Recherche de l'IP SQL Server..."
WINDOWS_IP=""
for ip in $(ip route | grep -oP '192\.168\.\d+\.\d+' | sort -u); do
    if nc -zw1 $ip 1433 2>/dev/null; then
        WINDOWS_IP=$ip
        break
    fi
done

if [ -z "$WINDOWS_IP" ]; then
    echo "❌ Impossible de trouver SQL Server"
    exit 1
fi

echo "✅ SQL Server trouvé : $WINDOWS_IP"

# Remplace localhost par l'IP dans tous les Default.properties
find ~/projet_mlops/etl/Master_Job_0.1 -name "Default.properties" | while read f; do
    sed -i "s/ServerName=localhost/ServerName=$WINDOWS_IP/" "$f"
    sed -i "s/ServerName=127\.0\.0\.1/ServerName=$WINDOWS_IP/" "$f"
done

# Lance le job
cd ~/projet_mlops/etl/Master_Job_0.1/Master_Job/
echo "🚀 Lancement ETL Talend..."
bash Master_Job_run.sh
echo "✅ ETL terminé"
