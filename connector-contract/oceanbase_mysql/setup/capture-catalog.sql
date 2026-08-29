SET NAMES utf8mb4 COLLATE utf8mb4_bin;
SET time_zone = '+00:00';

SELECT @@version AS server_version,
       @@version_comment AS version_comment,
       @@transaction_isolation AS transaction_isolation,
       @@ob_read_consistency AS observed_read_consistency,
       @@character_set_connection AS character_set_connection,
       @@collation_connection AS collation_connection,
       @@time_zone AS session_time_zone;

SELECT table_schema, table_name, table_collation
FROM information_schema.tables
WHERE table_schema = 'benchbase'
ORDER BY table_name;

SELECT table_name, ordinal_position, column_name, column_type, is_nullable,
       column_key, character_set_name, collation_name
FROM information_schema.columns
WHERE table_schema = 'benchbase'
ORDER BY table_name, ordinal_position;

SELECT grantee, privilege_type, table_schema, table_name
FROM information_schema.table_privileges
WHERE grantee LIKE '%blackbox%'
ORDER BY grantee, table_schema, table_name, privilege_type;
