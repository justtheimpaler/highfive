# HighFive

HighFive reads the data in the tables of a database schema and hashes it with the aim of comparing
it to a destination database schema where this data has been migrated. The destination database
can be of the same or different brand.

Its main goal is to compare data between different database vendors during a database migration.
In cases like these, typical vendor-specific tools that only work between instances of the same
database brand are not useful for the data verification.

The implemented strategy considers computing the hash values of all the data in both schemas using
the SHA-256 algorithm. Once this is done it becomes trivial to compare the hashed values between
schemas and decide if they fully match or not.

## Limitations

This tool has the following limitations.

### 1. Tables Must Be Sortable

Since hashing functions require ordered data sets, the ordering of rows is significant. This
means that the rows in both the source and destination databases must be hashed in the exact
same ordering.

To ensure a deterministic and stable ordering HighFive automatically detects the primary keys of the tables and
uses them for sorting by default.

If a table does not have a primary key it can still be hashed by declaring a sorting row ordering
for hashing using the property `<datasource>.hashing.ordering`. Ideally this sorting order should
produce no duplicate rows (for the ordering columns), so a unique constraint or unique index are
ideal for this purpose. If a hashing ordering is explicitly declared for a table, the primary key
is ignored and this hashing ordering takes precedence over it.

As an option of last resort, the full list of table columns could also be used for ordering, if
all the columns of the table are sortable. This may prove to be impractical or unrealistic due
to database resource constraints, particularly if the table has a massive number of rows and/or
has many heavy columns.

If none of the hashing ordering are practical, then the table can be excluded from HighFive using
the property `<datasource>.table.filter`. In this case, this table would fall outside the scope
of this tool and would need to be verified in a different way.

See the section **Hashing Ordering** for detail on how to declare specific orderings.

### 2. Supported Databases

This tool currently supports the following databases:

- Oracle
- DB2 LUW
- PostgreSQL
- SQL Server
- MySQL
- MariaDB

It's not difficult to add support for more databases (just add a new Dialect implementation).

### 3. Supported Data Types

This tool supports all common data types such as VARCHAR, NUMERIC (all variations), DATE,
TIMESTAMP w/wo TIME ZONE, BLOB, CLOB, etc. It does not support exotic types available is
some databases, such as GEOGRAPHY, MACADDR, POINT, arrays, range-like columns, record-like columns,
or pseudo-columns (ROWID, ROWNUM, OID, etc.).

It's not difficult to support more data types (just implement more serializers).

You can check if all data types in your schema are actually supported by running the `listtables`
command described below.

See **Appendix A - Supported Data Types** for the full list of supported types in each database.

### 4. Compares Identical Data With No Transformation

This tool compares the data in multiple databases in identical form. That is, it compares a VARCHAR
column to a VARCHAR column in another database, a NUMERIC column to a NUMERIC column in another database, and so forth. It does not account for data conversion or transformation. For example,
it cannot compare a BOOLEAN that is represented by a NUMERIC value in another database.

If data transformation needs to take place this tool can be useful to compare data in the destination database right after is transferred there, and ***before*** any transformation is applied.

### 5. Identifiers That Differ Only In Letter Case Are Not Supported

Since table names and column names may use a different letter case when migrating between
databases, the matching of table and column names is dome in a case-insensitive manner. That means
that the table `INVOICE` in one database will be successfully matched with the table `invoice` in
another database.

This is typically not a problem since rarely a database will have two tables with
the same name but different letter case. This tool does not support such a special case: that is,
for example, if a database schema had two tables such as `EMPLOYEE` and `employee` at the same time.

A similar case can happens in each table when identifying column names. A table with multiple
columns with different letter case can be created, but is not supported by this tool. For example,
the following table is not supported:

```sql
-- Although this is technically a valid table, it's not supported by this tool.
-- The columns "name" and "Name" differ only in the letter case.
create table INVOICE (
  id int,
  name varchar(20),
  "Name" varchar(20)
);

-- Also not supported, since two tables resolve to the same name in a case-insensitive form
create table "Invoice" (
  id int
);
```

In practice cases like these ones would be extremely rare. They are not case that we will be encounter
often -- or at all -- in business-grade databases.

## Usage

To use this tool you need:

- Access to the command line.
- Java 8 or newer installed.
- A folder where to install the tool, the JDBC driver JAR file(s), and the configuration file.

## Step 1 - Download this Tool

Get the tool from Maven Central at [search results](https://central.sonatype.com/search?q=highfive) and place it in the work folder.

### Step 2 - Download the JDBC Driver JAR file(s)

This application comes with no JDBC driver embedded.

Download the the JDBC drivers (JAR files) for the databases you want to connect to, and place
them in a folder of your choosing. Remember the folder and file names;
you'll use them in the next step when configuring the datasource(s).

The following table includes a sample list of JDBC drivers available in [Maven Central](https://central.sonatype.com/):

| Database | JDBC Driver |
| -- | -- |
| Oracle | https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar |
| DB2 LUW | https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.9.0/jcc-11.5.9.0.jar |
| PostgreSQL | https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar |
| SQL Server | https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre8/mssql-jdbc-12.2.0.jre8.jar |
| MySQL | https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar |
| MariaDB | https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/2.2.5/mariadb-java-client-2.2.5.jar |

**Note**: This is just a sample list of relatively old drivers. There are much newer versions of
them available for download. Make sure you download a driver for the Java version you are using.

### Step 3 - Configuration

Create the `application.properties` file in your folder. This file will define one or more
*datasources*. We will typically define one datasource for the source database and a second one
for the destination database.

A datasource name can only include upper/lower case letters, digits, and underscore symbols.

For example, if we name these datasources as `src` and `dest` respectively the configuration
file could look like:

```bash
# My old database

src.driver.jar=./lib/mssql-jdbc-12.2.0.jre8.jar
src.driver.class=com.microsoft.sqlserver.jdbc.SQLServerDriver
src.url=jdbc:sqlserver://...
src.username=myuser
src.password=mypass
src.catalog=mycatalog
src.schema=mychema

# My new database

dest.driver.jar=./lib/postgresql-42.2.5.jar
dest.driver.class=org.postgresql.Driver
dest.url=jdbc:postgresql://...
dest.username=otheruser
dest.password=otherpass
dest.catalog=
dest.schema=otherschema
```

You can add more datasources as needed. Each datasource has a unique name that will be used
in the commands when hashing or verifying.

The configurable properties are:

| Property | Description |
| --       | --          |
| `<datasource>.driver.jar`           | Path of the JDBC driver JAR library (that you downloaded) |
| `<datasource>.driver.class`         | The JDBC driver class name (see the database docs) |
| `<datasource>.url`                  | The database connection URL |
| `<datasource>.username`             | The database connection username |
| `<datasource>.password`             | The database connection password |
| `<datasource>.catalog`              | The catalog in the database. For SQL Server use the "database name" (such as `EN_IMG`); for PostgreSQL leave empty. This value is case-sensitive |
| `<datasource>.schema`               | The schema name. This value is case-sensitive |
| `<datasource>.table.filter`         | Optional. A list of comma-separated table names that will be included in the hash. Tables not mentioned in this list will be excluded from the hashing and validation. This value is case-insensitive |
| `<datasource>.remove.table.prefix`  | Optional. A prefix to be removed from the table names. If your table "client" was renamed as "old_client" and you still want it to be recorded in the hash file as "client", then specify "old_" in this property. This value is case-insensitive
| `<datasource>.column.filter`        | Optional. A list of comma-separated column names that will be included in the hash. Columns not mentioned in this list will be excluded from the hashing and validation. This value is case-insensitive |
| `<datasource>.max.rows`             | Optional. Limits the maximum number of rows per table to be hashed. Could be useful for test runs of for debugging if you want to work with a small and fast data set |
| `<datasource>.select.autocommit`    | Optional. Overrides the default autocommit mode for SELECT queries. PostgreSQL defaults to `false` while the other databases default to `true`. This autocommit mode is used to run large SELECT queries. For more details see the section **Autocommit While Reading** below |
| `<datasource>.type.rules`           | Optional. Rules to override the default Java types used for each database type; it's a semicolon-separated list of rules; each rule takes the form `<database-type>:<java-type>`. For example, if you want Oracle DATE columns to be read as java.time.LocalDate (instead of the default type java.time.LocalDateTime) use: `date:localdate`. Use the `listcolumns` command to show to the active Java types for each column according to the default or active rules. the rule definition is case-insensitive. For more details see the section **Type Rules** below |
| `<datasource>.hashing.ordering` | Optional. Declares hashing ordering. It overrides the primary key ordering for tables with primary keys, and declares a specific ordering for tables with no primary keys. It takes the form of a semicolon-separated list of table sorting rules, where each rule takes the form `<table>:<member>,<member>,...`; each member is a column name optionally followed by `/desc` to specify descending ordering on an index, and/or `/nf` or `/nl` to specify NULLS FIRST or NULLS LAST respectively ; nulls ordering may or not be supported by all databases. For more details see the section **Hashing Ordering** below |
| `<datasource>.readonly` | Optional. Declares this datasource as readonly (default) or writable. This property is  a safeguard to protect the datasources when copying data. A destination datasouce needs to be explicitly set as writable (`readonly=false`) for the `copy` command to work |
| `<datasource>.insert.batch.size` | Optional. Declares the insert batch size when copying data from one database to another. Defaults to 100 |


### Step 3 - Commands

HighFive implements the following commands:

| Command | Description |
| --  | -- |
| `listtables <datasource>` | Connects to the schema, list the tables in it, and checks they are all supported. Only tables and columns selected by the filters are considered. Useful to validate the connection and basic functionality |
| `listcolumns <datasource>` | Connects to the schema, list the tables and their columns in it and verify they are all supported. Only tables and columns selected by the filters are considered |
| `hash <datasource>` | Hashes the schema and saves the result to the file `<datasource>.hash` |
| `verify <datasource> <baseline-file>` | Hashes the schema and saves the result to the file `<datasource>.hash`. It then compares the computed hashed results with the *baseline-file* to decide if the comparison succees or fails |
| `copy <from-datasource> <to-datasource>` | Copies the data of the tables from a source datasource to a destination datasource. The destination datasource should not be readonly; that is, the property `<datasource>.readonly` should be explicitly set to `false`. The java types of the columns of the selected tables must match, even if the database types are different; use the `<datasource>.type.rules` to set java types explicitly. All database constraints and database auto-generated features should be disabled (or dropped) while the data is being copied |


## Examples

The following examples show how to check the specified schemas are supported by this tool, how to hash one of them, and how to validate against the other one.

These examples assume we have two datasources configured with names "src" (in a SQL Server database) and "dest" (in a PostgreSQL database) respectively. Generally speaking,
the ordering of the datasources is not important and we could validate from PostgreSQL into
SQL Server or even between databases of the same vendor, just by changing the order of execution of the commands.

More datasources can also be configured in the `application.properties` file to validate more than two schemas in any desired sequence.

### Example 1 - Validating the schemas

We can run a validation of the preconditions of both schemas using the command `listtables`. We can do:

```bash
$ java -jar highfive-1.2.0.jar listtables src
2024-08-12 11:54:32.110 INFO  - HighFive 1.2.0 - build 20240812-132812 - Command: List Tables
2024-08-12 11:54:32.111 INFO  -  
2024-08-12 11:54:32.426 INFO  - DataSource:
2024-08-12 11:54:32.426 INFO  -   name: src
2024-08-12 11:54:32.426 INFO  -   dialect: SQL Server
2024-08-12 11:54:32.426 INFO  -   url: jdbc:sqlserver://...
2024-08-12 11:54:32.427 INFO  -   username: myuser
2024-08-12 11:54:32.427 INFO  -   catalog: 
2024-08-12 11:54:32.427 INFO  -   schema: dbo
2024-08-12 11:54:32.531 INFO  -  
2024-08-12 11:54:32.531 INFO  - Tables found (2/2):
2024-08-12 11:54:32.531 INFO  -   client
2024-08-12 11:54:32.531 INFO  -   invoice
2024-08-12 11:54:32.531 INFO  -   payment
2024-08-12 11:54:32.608 INFO  -  
2024-08-12 11:54:32.608 INFO  - Summary of types found (4):
2024-08-12 11:54:32.608 INFO  -   date [localdate]: 1
2024-08-12 11:54:32.608 INFO  -   datetime2 [localdatetime]: 1
2024-08-12 11:54:32.608 INFO  -   int [integer]: 3
2024-08-12 11:54:32.608 INFO  -   varchar(20) [string]: 2
2024-08-12 11:54:32.608 INFO  -  
2024-08-12 11:54:32.608 INFO  - Row Count:
2024-08-12 11:54:32.612 INFO  -   client: 101782 rows
2024-08-12 11:54:32.615 INFO  -   invoice: 25668 rows
2024-08-12 11:54:32.615 INFO  -   payment: 22018 rows
2024-08-12 11:54:32.615 INFO  -  
2024-08-12 11:54:32.615 INFO  - Hashing preconditions:
2024-08-12 11:54:32.620 INFO  -   All tables found (3/3) - PASS
2024-08-12 11:54:32.620 INFO  -   All tables are sortable (3/3) - PASS
2024-08-12 11:54:32.620 INFO  -   Column types are supported (13/13) - PASS
2024-08-12 11:54:32.620 INFO  -   The schema can be hashed.
```

The same validation can be run in the destination schema just changing the last parameter:

```bash
$ java -jar highfive-1.2.0.jar listtables dest
2024-07-31 09:37:46.254 INFO  - HighFive 1.2.0 - build 20240812-132812 - Command: List Tables
2024-07-31 09:37:46.255 INFO  -  
2024-07-31 09:37:46.260 INFO  - Configuration:
2024-07-31 09:37:46.260 INFO  -   datasource: dest
2024-07-31 09:37:46.261 INFO  -   url: jdbc:postgresql://...
2024-07-31 09:37:46.261 INFO  -   username: otheruser
2024-07-31 09:37:46.261 INFO  -   catalog: 
2024-07-31 09:37:46.261 INFO  -   schema: otherschema
2024-07-31 09:37:46.727 INFO  -  
2024-07-31 09:37:46.727 INFO  - Tables found (3/3):
2024-07-31 09:37:46.727 INFO  -  - client
2024-07-31 09:37:46.727 INFO  -  - invoice
2024-07-31 09:37:46.727 INFO  -  - payment
2024-07-31 09:37:46.727 INFO  -  
2024-07-31 09:37:46.727 INFO  -  Row Count:
2024-07-31 09:37:46.727 INFO  -    client: 101782 rows
2024-07-31 09:37:46.727 INFO  -    invoice: 25668 rows
2024-07-31 09:37:46.727 INFO  -    payment: 22018 rows
2024-07-31 09:37:46.727 INFO  -  
2024-07-31 09:37:46.727 INFO  - Checking hashing preconditions:
2024-07-31 09:37:46.727 INFO  - Checking schema...
2024-07-31 09:37:47.311 INFO  -  
2024-07-31 09:37:47.312 INFO  - Hashing preconditions:
2024-07-31 09:37:47.312 INFO  -   All tables found (3/3) - PASS
2024-07-31 09:37:47.312 INFO  -   All tables are sortable (3/3) - PASS
2024-07-31 09:37:47.312 INFO  -   Column types are supported (13/13) - PASS
2024-07-31 09:37:47.312 INFO  -   The schema can be hashed.
```

### Example 2 - Hashing a Schema

We can hash the "src" schema by doing:

```bash
$ java -jar highfive-1.2.0.jar hash src
2024-07-31 09:42:44.236 INFO  - HighFive 1.2.0 - build 20240812-132812 - Command: Hash Data
2024-07-31 09:42:44.236 INFO  -  
2024-07-31 09:42:44.243 INFO  - Configuration:
2024-07-31 09:42:44.243 INFO  -   datasource: src
2024-07-31 09:42:44.243 INFO  -   url: jdbc:sqlserver://...
2024-07-31 09:42:44.243 INFO  -   username: myuser
2024-07-31 09:42:44.243 INFO  -   catalog: 
2024-07-31 09:42:44.243 INFO  -   schema: mychame
2024-07-31 09:42:44.584 INFO  -  
2024-07-31 09:42:44.626 INFO  - Reading table: client
2024-07-31 09:42:44.633 INFO  -   101782 row(s) read
2024-07-31 09:42:44.670 INFO  - Reading table: invoice
2024-07-31 09:42:44.737 INFO  -   25668 row(s) read
2024-07-31 09:42:44.811 INFO  - Reading table: payment
2024-07-31 09:42:44.813 INFO  -   22018 row(s) read
2024-07-31 09:42:44.814 INFO  -  
2024-07-31 09:42:44.814 INFO  - Data hashes generated to: src.hash
```

The hashes were saved to a hash file with the name of the datasource and the extension `.hash`; 
that is, to the file `src.hash`. The content of it looks like:

```bash
e37e7a4668c70da684b4e582c45589430be4d1a0 client
50745d7fb5fbd6b0b87f744aa459ac9e5091ae29 invoice
c6f0c8c409678d84c3325e751cbc999ecda68474 payment
```

These hashes will be compared to the destination table in the next step.

### Example 3 - Validating a Schema

To validate the data of the tables of a schema against precomputed hashed values you can do:

```bash
$ java -jar highfive-1.2.0.jar verify dest src.hash
2024-07-31 09:46:18.257 INFO  - HighFive 1.2.0 - build 20240812-132812 - Command: Verify Data
2024-07-31 09:46:18.257 INFO  -  
2024-07-31 09:46:18.264 INFO  - Configuration:
2024-07-31 09:46:18.264 INFO  -   datasource: dest
2024-07-31 09:46:18.264 INFO  -   url: jdbc:postgresql://...
2024-07-31 09:46:18.264 INFO  -   username: otheruser
2024-07-31 09:46:18.264 INFO  -   catalog: 
2024-07-31 09:46:18.264 INFO  -   schema: otherschema
2024-07-31 09:46:18.700 INFO  -  
2024-07-31 09:46:18.714 INFO  - Reading table: client
2024-07-31 09:46:18.725 INFO  -   101782 row(s) read
2024-07-31 09:46:18.732 INFO  - Reading table: invoice
2024-07-31 09:46:18.738 INFO  -   25668 row(s) read
2024-07-31 09:46:18.745 INFO  - Reading table: payment
2024-07-31 09:46:18.748 INFO  -   22018 row(s) read
2024-07-31 09:46:18.748 INFO  -  
2024-07-31 09:46:18.748 INFO  - Data hashes generated to: dest.hash
2024-07-31 09:46:18.748 INFO  -  
2024-07-31 09:46:18.749 INFO  - All data hashes match. The verification succeeded.
```

The verification compares the data in the live schema (specified as `dest`) with the previously
computed hashes (specified as the file `src.hash`).

The last line tells us that the verification succeeded. It only succeeds if all hashes fully match.

**Note**: The newly computed hashes were saved to a hash file with the name of the
datasource and the extension `.hash`; in this case this file is `dest.hash`. The content of
this file looks like:

```bash
e37e7a4668c70da684b4e582c45589430be4d1a0 client
50745d7fb5fbd6b0b87f744aa459ac9e5091ae29 invoice
c6f0c8c409678d84c3325e751cbc999ecda68474 payment
```

In this case it looks exactly as the previous one, and that's why the verification succeeded. If data corruption had taken place, you would see different values compared to the original one.

In case the data is different you can try to identify the offending data by:

- Narrowing down by table (by applying table filters) and comparing hashes.
- Narrowing down by table rows (by limiting the rows) and running successive hashing and verifications.

### Example 4 - Copying Data Between Databases

The `copy` command copies data between two databases, of the same or different brand in the supported list of
databases. These databases are specified in the command line by their datasource names.

Consider the following preconditions:

1. The destination datasource should be writable. That is, the property `<datasource>.readonly` should be
explicitly set to `false`. This property defaults to `true` when not specified.

1. The destination tables must be empty. Before copying the data the `copy` command checks that all affected tables
are empty. If data is found in these tables the copy stops.

1. All database constraints and database auto-generated features in the destination tables should be disabled
(or dropped) while the data is being copied.

1. The java types of the columns in the source and destination databases must match, even if the actual database
types are different. Use the `<datasource>.type.rules` to set java types explicitly in one or both datasources if
the default java types don't match. See the section **Type Rules** below for details on how to designate java types.

The following example illustrates how to copy data:

```bash
$ java -jar highfive-1.2.0 copy src dest
2024-08-13 11:01:00.181 INFO  - HighFive 1.2.0 - build 20240813-125250 - Command: Copy Data
2024-08-13 11:01:00.181 INFO  -  
2024-08-13 11:01:00.763 INFO  - Source Datasource:
2024-08-13 11:01:00.763 INFO  -   name: src
2024-08-13 11:01:00.764 INFO  -   dialect: SQL Server
2024-08-13 11:01:00.764 INFO  -   url: jdbc:sqlserver://...
2024-08-13 11:01:00.764 INFO  -   database: Microsoft SQL Server 12.00.5000
2024-08-13 11:01:00.764 INFO  -   JDBC Driver: Microsoft JDBC Driver 12.2 for SQL Server 12.2.0.0 - implements JDBC 4.2
2024-08-13 11:01:00.764 INFO  -   username: myuser
2024-08-13 11:01:00.764 INFO  -   catalog: 
2024-08-13 11:01:00.764 INFO  -   schema: dbo
2024-08-13 11:01:00.765 INFO  -  
2024-08-13 11:01:00.765 INFO  - Destination Datasource:
2024-08-13 11:01:00.765 INFO  -   name: dest
2024-08-13 11:01:00.765 INFO  -   dialect: PostgreSQL
2024-08-13 11:01:00.766 INFO  -   url: jdbc:postgresql://...
2024-08-13 11:01:00.766 INFO  -   database: PostgreSQL 16.1
2024-08-13 11:01:00.766 INFO  -   JDBC Driver: PostgreSQL JDBC Driver 42.2.5 - implements JDBC 4.2
2024-08-13 11:01:00.766 INFO  -   username: otheruser
2024-08-13 11:01:00.766 INFO  -   catalog: 
2024-08-13 11:01:00.766 INFO  -   schema: otherschema
2024-08-13 11:01:00.766 INFO  -   insert batch size: 100
2024-08-13 11:01:00.766 INFO  -  
2024-08-13 11:01:00.872 INFO  - Destination Database - Row Count:
2024-08-13 11:01:00.873 INFO  -   kitchen: 0 rows
2024-08-13 11:01:00.874 INFO  -   quadrant: 0 rows
2024-08-13 11:01:00.875 INFO  -  
2024-08-13 11:01:00.875 INFO  - Copying table client:
2024-08-13 11:01:00.968 INFO  -   101782 row(s) copied
2024-08-13 11:01:00.969 INFO  - Copying table invoice:
2024-08-13 11:01:01.001 INFO  -   25668 row(s) copied
2024-08-13 11:01:01.002 INFO  - Copying table payment:
2024-08-13 11:01:02.063 INFO  -   22018 row(s) copied
2024-08-13 11:01:02.063 INFO  - Copy complete -- Grand total of 9 row(s) copied
```

**Note**: There's an issue when copying data to MySQL. The MySQL Connector/J JDBC driver is buggy (at least up to version 9.0.0) when inserting a LocalDate or LocalDateTime into DATE or DATETIME columns respectively; it shifts the value by the time zone difference between the application and the database server/session if they are not perfectly aligned. This can end up inserting DATEs as the day before or day after, and DATETIMEs shifted forward or backward a few/many of hours. This bug does not affect MariaDB.

## Advanced Configuration

### 1. Autocommit While Reading

When reading large tables (tens of millions of rows and beyond) it's convenient to read the data
using buffering instead of retrieving the entire data in memory and then process it. When autocommit
is on, some database may be preventing from using buffering (e.g. PostgreSQL) so it's important to disable it to avoid loading the entire table in memory and possibly run into an OOO error.

The default value for autocommit for each database is:

| Database | Default Autocommit Value |
| -- | -- |
| Oracle | true |
| DB2 LUW| true |
| PostgreSQL | false |
| SQL Server | true |
| MySQL | true |
| MariaDB | true |

This value can be overriden on each datasource using the property `<datasource>.select.autocommit`.

### 2. Type Rules

In any application, there are multiple ways of reading a column of a table. For example a `DECIMAL(4, 0)` could be read as a `short`, as an `int`, or even as a `long` in Java. When it comes to hashing the value it's important to use the same Java representation to do so; otherwise the same value can
produce a different when read as an `short`, `int`, or `long`. That would defeat the purpose of the
hash comparison.

Most of the time the java types automatically selected by this tool are adequate and stable for hashing. However, there are some exceptions, particularly for database types that are old badly defined.

For example, the `DATE` type in the Oracle database seems to imply that stores dates without time, but in fact stores both. By default, this tool reads these data as `LocalDateTime` to preserve the time component of it. However, when the data is migrated only the date part makes it through. If you wanted to read `DATE` columns just as `LocalDate` you could do so by adding a rule in the form `<database-type>:<java-type>`; in this case this rule can look like:

```bash
  <datasource>.type.rules=date:localdate
```

One or more rules can be specified in the datasource as a semicolon-separated list of rules using the property `<datasource>.type.rules`. The rules are case-insensitive.

As an additional example, if you wanted to read all `DATE` columnas as `localdate` (discarding the
time component) and all `NUMBER(9, 0)` as `integer` values (assuming none exceeds 2^63-1) you could define the rule:

```bash
  <datasource>.type.rules=date:localdate;number(9, 0):integer
```

The available java-types are:

| Java Type | Description |
| -- | -- |
| string         | Used for text values (CHAR, VARCHAR, CLOBs, etc.) |
| integer    | Used for integer values with 9 digits or less (less than 2^32-1) |
| long           | Used for integer values between 9 and 18 digits |
| biginteger | Used for integer values that exceed 18 digits (greater than 2^63-1) |
| bigdecimal | Used for numeric values with decimal places (scale > 0) |
| double     | Used for floating point values (single or double precision) |
| localdate  | Used for date values without time component (and no time zone/offset) |
| localdatetime | Used for date values with time component (and no time zone/offset) |
| localtime     | Used for time values with no time offset |
| offsetdatetime  | Used for date values with time and time offset components |
| zoneddatetime  | Used for date values with time and time zone components |
| bytearray  | Used for binary data (BLOB, BINARY, VARBINARY, etc.) |
| boolean    | Used for boolean values (implemented in PostgreSQL) |

### 3. Hashing Ordering

When hashing the data of a table the ordering in which the rows are hashed is relevant. Therefore,
the same ordering needs to be used in all databases that are being compared.

If a table has a primary key, this is used as the default row ordering automatically.

Alternatively, a hashing ordering can be declared for a table. If the table already had a primary
key the declared ordering supersedes the primary key ordering.

The hashing ordering can be specified per datasource using the property
`<datasource>.hashing.ordering`. It takes the form of a semicolon-separated list of table sorting rules.

Each rule can take two forms:

- `<table>:*`: in this case all the columns of the table are used for sorting; the `ORDER BY`
clause includes them all in alphabetical order.
- `<table>:<member>,<member>,...`; each member is a column name optionally followed by `/desc` to specify descending ordering on an index, and/or `/nf` or `/nl` to specify NULLS FIRST or NULLS LAST respectively ; nulls ordering may or not be supported by all databases. This form can be specially
useful to target an existing [unique/non-unique] index or constraint where sorting is natural.

Ascending or descending ordering, as well as nulls ordering can have potential performance benefits
when using existing indexes with those properties.

Consider that the composition and ordering of columns in the `ORDER BY` clause can have performance
implications when reading the data using a SELECT query.

Also, the selected columns are used in the `ORDER BY` clause of the SELECT query and, as such, must be
sortable. This is usually the case for the most common data types such as CHAR, VARCHAR, NUMERIC,
DECIMAL, INT, BIGINT, DATE, TIMESTAMP, etc., but may not be the case for more exotic data types
such as `BOOLEAN`, `XML`, arrays, `IMAGE`, `BLOB`, etc.

**Note**: Unfortunately, deciding if a column is sortable can only be determined by the database
itself. For special and/or exotic data types this can only be found by actually trying to hash data
(maybe with a dry run with a limited number of rows).

The following example illustrates six different cases, for a datasource called `main`:

```sql
create table INVOICE ( -- Case #1 - Has a primary key
  ID int primary key,
  AMOUNT decimal(12, 2)
);

create table PRODUCT ( -- Case #2 - Has a unique constraint on a non-nullable column
  ID int primary key,
  NAME varchar(50) not null unique
);

create table EMPLOYEE ( -- Case #3 - Has a unique index on non-nullable columns
  FULL_NAME varchar(80),
  EMPLOYEE_NO varchar(16) not null
);
create unique index ix1 on EMPLOYEE (EMPLOYEE_NO);

create table CLIENT ( -- Case #4 - Has a unique constraint on multiple non-nullable columns
  BRANCH_ID int not null,
  CLIENT_NUMBER varchar(10) not null,
  NAME varchar(50),
  unique (BRANCH_ID, CLIENT_NUMBER)
);

create table PAYMENT ( -- Case #5 - No indexes, no constraints; can use all columns for sorting purposes
  RECEIVED_AT timestamp,
  AMOUNT decimal(12, 2),
  ACCOUNT_ID int
);

create table PROFILE ( -- Case #6 - There's no deterministic way of sorting
  CLIENT_ID int,
  RECORDED_AT timestamp,
  PHOTO blob
);
```

Then, the hashing ordering for the `main` datasource could be declared as:

```bash
main.hashing.ordering=product:name; employee:employee_no; client:branch_id,client_number; payment:*
```

In this case:

- The table `INVOICE` will be sorted by the primary key (`ID`). No need to declare it.
- The table `PRODUCT` will be sorted by the unique constraint (`NAME`). Since the ordering is declared, it supersedes the default ordering (by primary key).
- The table `EMPLOYEE` will be sorted by the unique index (`EMPLOYEE_NO`). It's necessary to declare it, since this table does not have a primary key.
- The table `CLIENT` will be sorted by the unique constraint (`BRANCH_ID`,`CLIENT_NUMBER`) as declared. It's necessary to declare it, since this table does not have a primary key.
- The table `PAYMENT` will be sorted using all its columns (in alphabetical sequence). That is by (`ACCOUNT_ID`, `AMOUNT`, `RECEIVED_AT`).
- The table `PROFILE` cannot be hashed since it does not have a primary key, unique constraint, or unique index. It's also not possible to sort by all its columns since one of them is not sortable (it's a `blob`). This table will need to be excluded from the hashing process.


## Comparison Strategy

### 1. Case Insensitive Identifiers

Since the table and column names can use different case by the nature of different databases, this
tool compares tables and columns ignoring the letter case in their names. That is, the table
"CLIENT" in one database will always be matched with the table "client" in another database.
This can be useful to match different database columns and tables. The same logic applied
to filtering table and column names.

### 2. Data Compared in Application

The data is not compared at rest inside the database storage. What matters is how the data is
retrieved and used by the applications. It's there where, for example, a DATE_OF_BIRTH must be read
correctly, regardless of the database engine, brand, version, or hosting provider. The data is
read from table columns using JDBC and the resulting values **in the application** are compared.
Not the values stored in the database storage; these ones can actually look quite different since
each database has different internal marshalling algorithms.

### 3. Serialization and Hashing

Once the data is read using JDBC it's converted to a binary representation. This binary conversion
is stable and is documented in each serializer class in the package `highfive/src/master/highfive/src/main/java/highfive/serializers`. The implementations are geared towards performance, in order to
provide fast hashing speeds when processing high volumes of data.

### 4. NULLs

Nulls are hashed with a specific byte-value, so they become significant in the result. This means
that, for example, a table with two nulls will hash differently than a table with a single null.

### 5. Hashing Algorithm

The SHA-256 algorithm was chosen by its speed and conciseness of the resulting values. This can be especially valuable when visual inspection of the resulting hash file is needed, typically
when data corruption is detected and it's usefule to narrow down the root cause of it.

Other stronger algorithms could also be implemented in the future but this version uses this one
only.

## Appendix A - Supported Data Types

### Oracle

The following data types are supported in Oracle:

- char, varchar2
- nchar, nvarchar2
- clob, nclob
- number
- float, binary_float, binary_double
- date (with time component)
- timestamp, timestamp with time zone, timestamp with local time zone
- blob
- raw, long raw

### DB2 LUW

The following data types are supported in DB2 LOW:

- char, varchar
- nchar, nvarchar
- clob, nclob
- dbclob
- graphic, vargraphic
- smallint, int, bigint
- numeric, decimal
- decfloat, real, float, double
- date
- time
- timestamp
- blob

### PostgreSQL

The following data types are supported in PostgreSQL:

- character (char), character varying (varchar)
- text
- smallint, integer, bigint
- decimal, numeric
- real, double precision
- date
- timestamp without time zone (timestamp)
- timestamp with time zone (timestamptz)
- time without time zone (time)
- bytea
- boolean

### SQL Server

The following data types are supported in SQL Server:

- char, varchar
- nchar, nvarchar
- text, ntext
- sysname
- decimal, numeric
- money, smallmoney
- tinyint, smallint, int, bigint
- bit
- float, real
- date
- datetime, smalldatetime, datetime2, datetimeoffset
- time
- binary, varbinary
- image
- uniqueidentifier
- xml

### MySQL

The following data types are supported in MySQL:

- char, varchar
- tinytext, text, mediumtext, longtext
- tinyint, smallint, mediumint, int, bigint (normal and unsigned)
- decimal
- float, double (normal and unsigned)
- date
- datetime
- timestamp
- time, year
- tinyblob, blob, mediumblob, longblob

### MariaDB

The following data types are supported in MariaDB:

- char, varchar
- tinytext, text, mediumtext, longtext
- tinyint, smallint, mediumint, int, bigint (normal and unsigned)
- decimal
- float, double (normal and unsigned)
- date
- datetime
- timestamp
- time, year
- tinyblob, blob, mediumblob, longblob
- bit




