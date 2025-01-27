# HashFive -- Version History

## 1.2.8:

- Fixed hashing of empty tables.
- Better messaging.

## 1.2.7:

- Analysis commands (hashd, hashc, and hashl) implemented.
- Automated detection of invalid sorting order while hashing.
- Added non-deterministic flag to the hash file.
- Command hashdupes implemented.

## 1.2.5:

- Implemented collations in sort ordering while hashing.
- Hashing now continues on SQL errors, displaying the error messages.
- Cursors are now enabled by default in PostgreSQL when selecting.

## 1.2.4:

- Implemented cursors while hashing.
- Switched to JUL instead of Log4j.
- Fixed PostgreSQL primary key discovery.

## 1.2.3:

- Implemented SQL query log feature.

## 1.2.2:

- Fixing limiting in PostgreSQL.

## 1.2.1:

- Adding better handling of reserved words.
- Adding support for non-standard identifiers that require quoting.

## 1.2.0:

- The copy command is now implemented.
- All JDBC drivers are now externalized.
- Supports for six databases: Oracle, DB2 LUW, PostgreSQL, SQL Server, MySQL, and MariaDB.

## 1.1.0:

- Initial version that implements hashing.
- Commands hash and verify are implemented.
- Diagnostic commands listtables and listcolums are implemented.

