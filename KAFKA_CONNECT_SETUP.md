# Kafka Connect Setup Guide

## Overview
This guide walks through setting up Kafka Connect on a Debian machine (192.168.1.73) to connect to the Kafka cluster and PostgreSQL database.

**Environment Details:**
- Kafka Connect Host: 192.168.1.73
- Kafka Cluster: 192.168.1.72:29092 (Confluent 7.4.4)
- Schema Registry: 192.168.1.72:8081
- PostgreSQL Host: 192.168.1.70
- PostgreSQL User: postgres
- PostgreSQL Password: Adobe123!
- Kafka Connect: 7.7.0 (backward compatible with 7.4.4 cluster)

**Software Versions (Latest as of Feb 2026):**
- Java: OpenJDK 21 LTS
- Confluent Platform: 7.7.0
- PostgreSQL JDBC Driver: 42.7.4
- JDBC Sink Connector: 10.8.0

---

## Step 1: Install Java

Kafka Connect requires Java 11 or later. We'll install Java 21 LTS for best performance.

```bash
# Update package list
sudo apt update

# Install OpenJDK 21 (LTS) and useful utilities
sudo apt install -y openjdk-21-jdk jq curl wget

# Verify installations
java -version
jq --version
```

---

## Step 2: Download and Install Confluent Platform

We'll use Confluent Platform 7.7.0 (latest stable release).

```bash
# Create installation directory
sudo mkdir -p /opt/confluent
cd /opt/confluent

# Download Confluent Platform 7.7.0
wget https://packages.confluent.io/archive/7.7/confluent-community-7.7.0.tar.gz

# Extract
sudo tar -xzf confluent-community-7.7.0.tar.gz
sudo mv confluent-7.7.0 /opt/confluent/

# Create symlink for easier access
sudo ln -s /opt/confluent/confluent-7.7.0 /opt/confluent/current

# Add to PATH (add to ~/.bashrc for persistence)
export CONFLUENT_HOME=/opt/confluent/current
export PATH=$PATH:$CONFLUENT_HOME/bin
```

Make the PATH permanent:
```bash
echo 'export CONFLUENT_HOME=/opt/confluent/current' >> ~/.bashrc
echo 'export PATH=$PATH:$CONFLUENT_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

---

## Step 3: Install Confluent Hub Client

The `confluent-hub` CLI is not included in the Community tarball, so we need to install it separately:

```bash
# Ensure environment variables are set
export CONFLUENT_HOME=/opt/confluent/current
export PATH=$PATH:$CONFLUENT_HOME/bin

# Download and install Confluent Hub Client
cd /tmp
wget https://client.hub.confluent.io/confluent-hub-client-latest.tar.gz

# Extract
tar -xzf confluent-hub-client-latest.tar.gz

# Move both the script and JAR files to Confluent installation
sudo cp -r bin/* $CONFLUENT_HOME/bin/
sudo mkdir -p $CONFLUENT_HOME/share/java/confluent-hub-client
sudo cp -r share/java/confluent-hub-client/* $CONFLUENT_HOME/share/java/confluent-hub-client/

# Make the script executable
sudo chmod +x $CONFLUENT_HOME/bin/confluent-hub

# Verify installation
confluent-hub help
```

---

## Step 4: Download PostgreSQL JDBC Driver and Connector

```bash
# Create connectors directory
sudo mkdir -p $CONFLUENT_HOME/share/confluent-hub-components

# Download PostgreSQL JDBC driver (latest version)
cd /tmp
wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Move to Kafka Connect libs
sudo mkdir -p $CONFLUENT_HOME/share/java/kafka-connect-jdbc
sudo cp postgresql-42.7.4.jar $CONFLUENT_HOME/share/java/kafka-connect-jdbc/

# Install Confluent JDBC connector using confluent-hub (latest version)
confluent-hub install confluentinc/kafka-connect-jdbc:10.8.0 --component-dir $CONFLUENT_HOME/share/confluent-hub-components --no-prompt

# Install Avro Converter (needed for Schema Registry integration)
confluent-hub install confluentinc/kafka-connect-avro-converter:7.7.0 --component-dir $CONFLUENT_HOME/share/confluent-hub-components --no-prompt
```

---

## Step 5: Configure Kafka Connect

Create the Kafka Connect configuration file:

```bash
sudo mkdir -p /etc/kafka-connect
sudo nano /etc/kafka-connect/connect-distributed.properties
```

Add the following configuration:

```properties
# Kafka broker configuration
bootstrap.servers=192.168.1.72:29092

# Connect configuration
group.id=connect-cluster
key.converter=io.confluent.connect.avro.AvroConverter
value.converter=io.confluent.connect.avro.AvroConverter
key.converter.schema.registry.url=http://192.168.1.72:8081
value.converter.schema.registry.url=http://192.168.1.72:8081

# Internal topics configuration
config.storage.topic=connect-configs
config.storage.replication.factor=1
offset.storage.topic=connect-offsets
offset.storage.replication.factor=1
status.storage.topic=connect-status
status.storage.replication.factor=1

# REST API configuration
rest.port=8083
rest.advertised.host.name=192.168.1.73

# Plugin path - include both locations
plugin.path=/opt/confluent/current/share/java,/opt/confluent/current/share/confluent-hub-components

# Connector configuration
connector.client.config.override.policy=All
```

---

## Step 6: Set Up PostgreSQL Database

Create a database and table to receive Kafka messages:

```bash
# Connect to PostgreSQL
psql -U postgres -h 192.168.1.70

# Create database
CREATE DATABASE sample_db;

# Connect to the database
\c sample_db

# Create schema
CREATE SCHEMA IF NOT EXISTS kafka;

# Note: With auto.create=true and auto.evolve=true in the connector config,
# the table will be created automatically. However, you can pre-create it
# for better control over data types and constraints.
#
# Option 1: Let the connector create the table automatically (skip table creation)
# Option 2: Pre-create a simple table and let auto.evolve add columns as needed:

CREATE TABLE IF NOT EXISTS kafka.incoming_messages (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

# Option 3: Create the full table manually with all fields (for maximum control):
# Uncomment the following if you prefer to define all columns explicitly:
#
# DROP TABLE IF EXISTS kafka.incoming_messages;
# CREATE TABLE kafka.incoming_messages (
#     id SERIAL PRIMARY KEY,
#     hene_id VARCHAR(255),
#     deliveryTemplate VARCHAR(255),
#     partyid VARCHAR(255),
#     brand VARCHAR(255),
#     email VARCHAR(255),
#     firstName VARCHAR(255),
#     lastName VARCHAR(255),
#     language VARCHAR(50),
#     string1 VARCHAR(500),
#     string2 VARCHAR(500),
#     string3 VARCHAR(500),
#     string4 VARCHAR(500),
#     string5 VARCHAR(500),
#     string6 VARCHAR(500),
#     string7 VARCHAR(500),
#     string8 VARCHAR(500),
#     string9 VARCHAR(500),
#     string10 VARCHAR(500),
#     int1 INTEGER,
#     int2 INTEGER,
#     int3 INTEGER,
#     int4 INTEGER,
#     int5 INTEGER,
#     int6 INTEGER,
#     int7 INTEGER,
#     int8 INTEGER,
#     int9 INTEGER,
#     int10 INTEGER,
#     datetime1 BIGINT,
#     datetime2 BIGINT,
#     datetime3 BIGINT,
#     datetime4 BIGINT,
#     datetime5 BIGINT,
#     datetime6 BIGINT,
#     datetime7 BIGINT,
#     datetime8 BIGINT,
#     datetime9 BIGINT,
#     datetime10 BIGINT,
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );

# Grant permissions
GRANT ALL PRIVILEGES ON DATABASE sample_db TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA kafka TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA kafka TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA kafka TO postgres;

# Exit
\q
```

---

## Step 7: Create Systemd Service for Kafka Connect

Create a systemd service file for auto-start:

```bash
sudo nano /etc/systemd/system/kafka-connect.service
```

Add the following content:

```ini
[Unit]
Description=Apache Kafka Connect
After=network.target

[Service]
Type=simple
User=root
Environment="CONFLUENT_HOME=/opt/confluent/current"
Environment="KAFKA_HEAP_OPTS=-Xms512M -Xmx2G"
ExecStart=/opt/confluent/current/bin/connect-distributed /etc/kafka-connect/connect-distributed.properties
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable kafka-connect

# Start the service
sudo systemctl start kafka-connect

# Check status
sudo systemctl status kafka-connect

# View logs
sudo journalctl -u kafka-connect -f
```

---

## Step 8: Create PostgreSQL Sink Connector

Wait for Kafka Connect to fully start (check logs), then create the sink connector:

```bash
# Create connector configuration file
cat > /tmp/postgres-sink.json << 'EOF'
{
  "name": "postgres-sink-incoming-messages",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "incoming_messages",
    "connection.url": "jdbc:postgresql://192.168.1.70:5432/sample_db",
    "connection.user": "postgres",
    "connection.password": "Adobe123!",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "insert",
    "table.name.format": "kafka.incoming_messages",
    "pk.mode": "none",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://192.168.1.72:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://192.168.1.72:8081",
    "transforms": "flatten",
    "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
    "transforms.flatten.delimiter": "_"
  }
}
EOF

# Submit the connector
curl -X POST -H "Content-Type: application/json" \
  --data @/tmp/postgres-sink.json \
  http://192.168.1.73:8083/connectors
```

---

## Step 9: Verify and Monitor

### Check Connector Status
```bash
# List all connectors
curl http://192.168.1.73:8083/connectors

# Check specific connector status
curl http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/status

# View connector configuration
curl http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages
```

### Monitor Data Flow
```bash
# Check PostgreSQL for incoming data
psql -U postgres -h 192.168.1.70 -d sample_db -c "SELECT COUNT(*) FROM kafka.incoming_messages;"

# View recent records
psql -U postgres -h 192.168.1.70 -d sample_db -c "SELECT * FROM kafka.incoming_messages ORDER BY created_at DESC LIMIT 10;"
```

### View Kafka Connect Logs
```bash
# Real-time logs
sudo journalctl -u kafka-connect -f

# Last 100 lines
sudo journalctl -u kafka-connect -n 100

# Errors only
sudo journalctl -u kafka-connect -p err
```

---

## Step 10: Common Operations

### Restart Connector
```bash
curl -X POST http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/restart
```

### Pause Connector
```bash
curl -X PUT http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/pause
```

### Resume Connector
```bash
curl -X PUT http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/resume
```

### Delete Connector
```bash
curl -X DELETE http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages
```

### Update Connector Configuration
```bash
# Method 1: Edit the existing configuration file and update (requires jq)
nano /tmp/postgres-sink.json

# After making changes, update the connector (only sends the "config" portion)
curl -X PUT -H "Content-Type: application/json" \
  --data "$(jq '.config' /tmp/postgres-sink.json)" \
  http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/config

# Method 2: Without jq - Create a config-only file (no "name" field)
cat > /tmp/connector-config.json << 'EOF'
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "tasks.max": "2",
  "topics": "incoming_messages",
  "connection.url": "jdbc:postgresql://192.168.1.70:5432/sample_db",
  "connection.user": "postgres",
  "connection.password": "Adobe123!",
  "auto.create": "true",
  "auto.evolve": "true",
  "insert.mode": "insert",
  "table.name.format": "kafka.incoming_messages",
  "pk.mode": "none",
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://192.168.1.72:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://192.168.1.72:8081",
  "transforms": "flatten",
  "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
  "transforms.flatten.delimiter": "_"
}
EOF

# Then update the connector
curl -X PUT -H "Content-Type: application/json" \
  --data @/tmp/connector-config.json \
  http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/config

# Verify the update was applied
curl http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/config
```

**Note:** 
- The `/config` endpoint expects only the configuration object, not the full connector JSON with `name` field
- The connector will restart automatically after configuration update
- Changes take effect immediately
- If you don't have `jq` installed: `sudo apt install -y jq`

---

## Troubleshooting

### Issue: jq command not found
If you see `jq: command not found`, install it:
```bash
sudo apt update
sudo apt install -y jq
```

Alternatively, use the commands without jq (grep/plain curl alternatives are provided throughout this guide).

### Issue: Understanding auto.create and auto.evolve
The connector is configured with `auto.create: true` and `auto.evolve: true` by default. This means:

- **auto.create: true** - The connector will automatically create the table if it doesn't exist
- **auto.evolve: true** - The connector will automatically add new columns when new fields appear in the Avro schema

**When to disable:**
- Set `auto.create: false` if you want full control over table creation (indexes, constraints, etc.)
- Set `auto.evolve: false` in production environments where schema changes should be reviewed

**To disable automatic schema management:**
```json
{
  "auto.create": "false",
  "auto.evolve": "false"
}
```
Then you must manually create the table with all required fields (see Option 3 in Step 6).

### Issue: Table already exists with wrong schema
If you created the table before with different columns:
```bash
psql -U postgres -h 192.168.1.70 -d sample_db
DROP TABLE IF EXISTS kafka.incoming_messages;
# Then restart the connector to let it recreate the table automatically
\q
```

Or manually create it with the full schema from Step 6 Option 3, and set `auto.create: false, auto.evolve: false` in connector config.

### Issue: Cannot ALTER TABLE to add missing field
**Error:** `Cannot ALTER TABLE to add missing field SinkRecordField{...name='partyid'...} as the field is not optional and does not have a default value`

**Cause:** The table already exists but is missing required (non-nullable) fields from your Avro schema. Even with `auto.evolve: true`, the connector can't add required fields to an existing table because PostgreSQL requires them to be nullable or have a default value.

**Solution 1: Drop and recreate (Recommended - Quickest fix)**

Let the connector create the table from scratch with all fields:

```bash
# 1. Delete the existing connector
curl -X DELETE http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages

# 2. Drop the table
psql -U postgres -h 192.168.1.70 -d sample_db -c "DROP TABLE IF EXISTS kafka.incoming_messages;"

# 3. Recreate the connector (it will auto-create the table with all fields)
curl -X POST -H "Content-Type: application/json" \
  --data @/tmp/postgres-sink.json \
  http://192.168.1.73:8083/connectors

# 4. Check connector status
curl http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/status

# 5. Verify table was created with all columns
psql -U postgres -h 192.168.1.70 -d sample_db -c "\d kafka.incoming_messages"
```

**Solution 2: Manually add missing columns (If you want to keep existing data)**

```bash
# 1. Pause the connector
curl -X PUT http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/pause

# 2. Add all missing columns as nullable
psql -U postgres -h 192.168.1.70 -d sample_db << 'EOF'
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS hene_id VARCHAR(255);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS deliveryTemplate VARCHAR(255);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS partyid VARCHAR(255);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS brand VARCHAR(255);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS email VARCHAR(255);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS firstName VARCHAR(255);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS lastName VARCHAR(255);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS language VARCHAR(50);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string1 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string2 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string3 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string4 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string5 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string6 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string7 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string8 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string9 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS string10 VARCHAR(500);
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int1 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int2 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int3 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int4 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int5 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int6 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int7 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int8 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int9 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS int10 INTEGER;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime1 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime2 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime3 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime4 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime5 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime6 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime7 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime8 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime9 BIGINT;
ALTER TABLE kafka.incoming_messages ADD COLUMN IF NOT EXISTS datetime10 BIGINT;
EOF

# 3. Resume the connector
curl -X PUT http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/resume

# 4. Check status
curl http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/status
```

**Why this happens:**
- You likely created a minimal table first (Step 6 Option 2 with just `id` and `created_at`)
- Your Avro schema has required fields like `partyid` defined as `"type": "string"` (not nullable)
- PostgreSQL won't let you add non-nullable columns without a default value to an existing table
- The connector's `auto.evolve` can only add nullable columns to existing tables

**Prevention for next time:**
Either use Step 6 Option 1 (skip table creation, let connector do it) or ensure `auto.create: true` when the table doesn't exist yet.

### Issue: Connector fails to start
**Solution:**
1. Check logs: `sudo journalctl -u kafka-connect -n 200`
2. Verify PostgreSQL connectivity:
   ```bash
   psql -U postgres -h 192.168.1.70 -d sample_db -c "SELECT 1;"
   ```
3. Test Kafka connectivity:
   ```bash
   $CONFLUENT_HOME/bin/kafka-console-consumer --bootstrap-server 192.168.1.72:29092 --topic incoming_messages --from-beginning --max-messages 1
   ```

### Issue: Schema Registry connection errors
**Solution:**
1. Test Schema Registry:
   ```bash
   curl http://192.168.1.72:8081/subjects
   ```
2. Verify Avro converter is properly installed
3. Check Schema Registry URL in connector config

### Issue: JDBC driver not found
**Solution:**
```bash
# Verify driver location
ls -la $CONFLUENT_HOME/share/java/kafka-connect-jdbc/

# Reinstall if needed
sudo cp /tmp/postgresql-42.7.4.jar $CONFLUENT_HOME/share/java/kafka-connect-jdbc/
sudo systemctl restart kafka-connect
```

### Issue: Authentication failed to PostgreSQL
**Solution:**
1. Check PostgreSQL pg_hba.conf:
   ```bash
   sudo nano /etc/postgresql/*/main/pg_hba.conf
   ```
2. Ensure there's a line allowing password authentication:
   ```
   host    all             all             192.168.1.0/24          md5
   ```
3. Restart PostgreSQL:
   ```bash
   sudo systemctl restart postgresql
   ```

### Issue: No data flowing to PostgreSQL
**Solution:**
1. Check if topic has data:
   ```bash
   $CONFLUENT_HOME/bin/kafka-console-consumer --bootstrap-server 192.168.1.72:29092 --topic incoming_messages --from-beginning --max-messages 5
   ```
2. Verify connector is running:
   ```bash
   curl http://192.168.1.73:8083/connectors/postgres-sink-incoming-messages/status
   ```
3. Check for schema compatibility issues in logs

### Issue: Out of Memory errors
**Solution:**
Increase heap size:
```bash
sudo nano /etc/systemd/system/kafka-connect.service
```
Change:
```ini
Environment="KAFKA_HEAP_OPTS=-Xms1G -Xmx4G"
```
Then:
```bash
sudo systemctl daemon-reload
sudo systemctl restart kafka-connect
```

---

## Advanced Configuration

### Custom Table Mapping
If you need to map Avro fields to different PostgreSQL columns:

```json
{
  "name": "postgres-sink-custom",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "incoming_messages",
    "connection.url": "jdbc:postgresql://192.168.1.70:5432/sample_db",
    "connection.user": "postgres",
    "connection.password": "Adobe123!",
    "auto.create": "false",
    "insert.mode": "insert",
    "table.name.format": "kafka.incoming_messages",
    "pk.mode": "record_value",
    "pk.fields": "hene_id",
    "fields.whitelist": "hene_id,deliveryTemplate,partyid",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://192.168.1.72:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://192.168.1.72:8081"
  }
}
```

### Error Handling Configuration
Add error tolerance:

```json
{
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.deadletterqueue.topic.name": "dlq-incoming-messages",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

---

## Performance Tuning

### Increase Throughput
```json
{
  "tasks.max": "3",
  "batch.size": "3000",
  "consumer.max.poll.records": "500",
  "consumer.max.poll.interval.ms": "300000"
}
```

### Connection Pool Settings
```json
{
  "connection.attempts": "3",
  "connection.backoff.ms": "10000"
}
```

---

## Security Considerations

1. **Use Environment Variables for Secrets:**
   Store passwords in a secure location and reference them in connector configs.

2. **Enable SSL for PostgreSQL:**
   Update connection URL:
   ```
   jdbc:postgresql://192.168.1.70:5432/sample_db?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory
   ```

3. **Enable SASL/SSL for Kafka:**
   If your Kafka cluster uses authentication, update the worker configuration.

4. **Restrict Network Access:**
   Use firewall rules to restrict access to Kafka Connect REST API:
   ```bash
   sudo ufw allow from 192.168.1.0/24 to any port 8083
   ```

---

## Backup and Recovery

### Backup Connector Configurations
```bash
# Create backup directory
mkdir -p /backup

# Export all connectors (requires jq)
for connector in $(curl -s http://192.168.1.73:8083/connectors | jq -r '.[]'); do
  curl -s http://192.168.1.73:8083/connectors/$connector | jq '.' > /backup/$connector.json
done

# Alternative without jq (exports raw JSON)
for connector in $(curl -s http://192.168.1.73:8083/connectors | grep -o '"[^"]*"' | tr -d '"'); do
  curl -s http://192.168.1.73:8083/connectors/$connector > /backup/$connector.json
done
```

### Restore Connectors
```bash
# Restore all connectors
for file in /backup/*.json; do
  curl -X POST -H "Content-Type: application/json" --data @$file http://192.168.1.73:8083/connectors
done
```

---

## Next Steps

1. **Monitor Performance:** Set up monitoring with Prometheus/Grafana
2. **Scale Out:** Add more Kafka Connect workers for high throughput
3. **Add More Connectors:** Connect to other data sources/sinks
4. **Implement CI/CD:** Automate connector deployment
5. **Set Up Alerting:** Alert on connector failures or lag

---

## Useful Commands Reference

```bash
# Install jq if not already installed
sudo apt install -y jq

# Kafka Connect Service
sudo systemctl start kafka-connect
sudo systemctl stop kafka-connect
sudo systemctl restart kafka-connect
sudo systemctl status kafka-connect
sudo journalctl -u kafka-connect -f

# Connector Management
curl http://192.168.1.73:8083/connectors                                    # List
curl http://192.168.1.73:8083/connectors/<name>/status                      # Status
curl -X POST http://192.168.1.73:8083/connectors/<name>/restart             # Restart
curl -X PUT http://192.168.1.73:8083/connectors/<name>/pause                # Pause
curl -X PUT http://192.168.1.73:8083/connectors/<name>/resume               # Resume
curl -X DELETE http://192.168.1.73:8083/connectors/<name>                   # Delete

# View connector details (pretty print with jq, or raw without)
curl http://192.168.1.73:8083/connectors/<name> | jq
curl http://192.168.1.73:8083/connectors/<name>

# PostgreSQL
psql -U postgres -h 192.168.1.70 -d sample_db
\dt kafka.*                                                                  # List tables
SELECT COUNT(*) FROM kafka.incoming_messages;                                # Count records

# Kafka Topics
$CONFLUENT_HOME/bin/kafka-topics --bootstrap-server 192.168.1.72:29092 --list
$CONFLUENT_HOME/bin/kafka-console-consumer --bootstrap-server 192.168.1.72:29092 --topic incoming_messages --from-beginning
```

---

## Additional Resources

- [Confluent Kafka Connect Documentation](https://docs.confluent.io/platform/current/connect/index.html)
- [JDBC Sink Connector Documentation](https://docs.confluent.io/kafka-connect-jdbc/current/sink-connector/index.html)
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/)

---

**Setup completed by:** GitHub Copilot  
**Date:** February 20, 2026  
**Version:** 1.0
