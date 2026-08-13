-- Debezium: snapshot + binlog streaming.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium_pw';
GRANT SELECT, RELOAD, SHOW DATABASES, LOCK TABLES,
      REPLICATION SLAVE, REPLICATION CLIENT, BINLOG MONITOR
  ON *.* TO 'debezium'@'%';

-- Workload client.
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY 'app_pw';
GRANT ALL PRIVILEGES ON sitest.* TO 'app'@'%';
