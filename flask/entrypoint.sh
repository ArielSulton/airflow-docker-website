#!/bin/bash
set -e

# Configuration
MAX_RETRIES=30
RETRY_INTERVAL=2

# Wait for PostgreSQL
wait_for_postgres() {
    echo "Waiting for PostgreSQL to be ready..."
    local retry_count=0
    
    while ! nc -z "$POSTGRES_HOST" "$POSTGRES_PORT"; do
        retry_count=$((retry_count + 1))
        
        if [ $retry_count -ge $MAX_RETRIES ]; then
            echo "Error: Failed to connect to PostgreSQL after $MAX_RETRIES attempts"
            exit 1
        fi
        
        echo "Attempt $retry_count/$MAX_RETRIES: PostgreSQL is unavailable - sleeping ${RETRY_INTERVAL}s"
        sleep $RETRY_INTERVAL
    done
    
    echo "PostgreSQL is ready!"
}

main() {
    wait_for_postgres

    # 'docker exec -it flask /bin/bash' exec once
    # flask db init 
    # flask db migrate -m "initial migration"
    # flask db upgrade 
    
    echo "Starting Flask application..."
    exec flask run --host=0.0.0.0 --port=8080
}

main "$@"