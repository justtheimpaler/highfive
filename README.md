# HighFive

HighFive helps with migrating data from one database to another, especially when these databases are of different vendors.

One one side, HighFive can **copy the data from one database to another**. For example, it can be used to migrate the data from an Oracle database to a PostgreSQL database (or vice versa).

HighFive comes with a default data type conversion strategy that can be customized. When the databases belong to different vendors (e.g. a database migration), typical vendor-specific tools that only work between instances of the same database brand are not useful for data copying or verification. Third-party tools that specialize in this scenarios can have high licensing costs. HighFive can be a great fit for the most common cases, when the databases do not include exotic features such as special data types, unorthodox table names or column names.

On the other side, HighFive can **compare the data between two (or more) databases**. This is particularly useful to verify the data was copied correctly to a destination database -- by this tool or by another one.

It performs the comparison by computing hash values for each table in one database (the "baseline database") and then by computing again the hash values in the other database(s). If the data was copied correctly these hashes will fully match. The implemented strategy considers computing the hash values of all the data in both schemas using the SHA-256 algorithm. Once this is done it becomes trivial to compare the hashed values between schemas and decide if they fully match or not.

## Limitations

This tool has the following limitations.

### 1. Supported Databases

This tool currently supports the following databases:

- Oracle
- DB2 LUW
- PostgreSQL
- SQL Server
- MySQL
- MariaDB


### 2. Does Not Migrate Schemas And Tables Structures

The data copy functionality does not migrate schema and table's structures. When copying data between databases this tool expects the schema and tables in the destination one to be already present. These tables must also be empty, and with any constraints removed or disabled to prevent any error when inserting data.

There are other tools in the market that migrate the structure of the schemas from one database to another; they typically create a schema with empty tables in the destination database. For example: the AWS Schema Convertion Tool (free and works quite well), Luna Modeler (paid edition), and other ETL tools.

Once the table structures are ready HighFive can start migrating data.


### 3. To Verify Data The Tables Must Be Sortable

For data verification process to be able to work, the tables must be sortable. This only required for data verification purposes, not when copying data.

Since the hashing functions used for verification require ordered data sets, the ordering of the retrieved rows is significant. This means that the rows in both the source and destination databases must be read and hashed in the exact same ordering.

To ensure a deterministic and stable ordering HighFive can:

1. Automatically detect the **primary keys** of the tables and use them by default for sorting purposes.
2. If a table does not have a primary key it can still be hashed by **declaring a list of columns** for sorting purposes using the property `<datasource>.hashing.ordering`. Ideally this sorting order should
produce no duplicate rows for the ordering columns, so a non-nullable unique constraint or index is ideal for this purpose. If a hashing ordering is explicitly declared for a table, the primary key is ignored and this hashing ordering is used instead.
3. As an option of last resort, **all the columns of the table** can also be used for ordering. This can work as long as all the columns of the table are actually sortable. Even then, this solution may prove impractical or unrealistic due to database resource constraints, particularly if the table has a massive number of rows (the engine may run out of resources while sorting) and/or has many heavy columns (it could take a very long time to sort data).

**Note**: In some cases the sorting order of different databases can differ. This is typically the case for primary keys (or other sorting criteria) that include CHAR/VARCHAR columns that have incompatible/mismatching collations between databases. If this is the case, specify compatible collations for both databases explicitly in the configuration of the corresponding datasources.

Finally, if none of the hashing ordering are practical for a table, the table can be excluded from the verification using the property `<datasource>.table.filter`. In this case, this table would fall outside the scope of this tool and would need to be verified in a different way.

See the section **Hashing Ordering** for details on how to declare specific orderings.

### 4. Supported Data Types

This tool supports all common data types such as VARCHAR, NUMERIC (all variations), DATE,
TIMESTAMP w/wo TIME ZONE, BLOB, CLOB, etc. It does not support exotic types available is
some databases, such as GEOGRAPHY, MACADDR, POINT, arrays, range-like columns, record-like columns,
or pseudo-columns (ROWID, ROWNUM, OID, etc.).

It's not difficult to support more data types (just implement more serializers).

You can check if all data types in your schema are actually supported by running the `listtables`
command described below.

See **Appendix A - Supported Data Types** for the full list of supported types in each database.

### 5. Compares Identical Data With No Transformation

This tool compares the data in multiple databases in identical form. That is, it compares a VARCHAR
column to a VARCHAR column in another database, a NUMERIC column to a NUMERIC column in another database, and so forth. It does not account for data conversion or transformation. For example,
it cannot compare a BOOLEAN that is represented by a NUMERIC value in another database.

If data transformation needs to take place this tool can be useful to compare data in the destination database right after is transferred there, and ***before*** any transformation is applied.

### 6. Identifiers That Differ Only In Letter Case Are Not Supported

Since table names and column names may use a different letter case when migrating between
databases, the matching of table and column names is done in a case-insensitive manner. That means
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

In practice cases like these ones would be extremely rare. They are not cases that we will be encounter
often -- or at all -- in business-grade databases.

## Usage

To use this tool you need:

- Access to the command line.
- Java 8 or newer installed.
- A folder (the working folder) where to install the tool, the JDBC driver JAR file(s), and the configuration file.

To use it follow the steps:

### Step 1 - Download this Tool

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
| `<datasource>.driver.jar`           | Path to the JDBC driver JAR library (that you downloaded) |
| `<datasource>.driver.class`         | The JDBC driver class name (see the database docs) |
| `<datasource>.url`                  | The database connection URL |
| `<datasource>.username`             | The database connection username |
| `<datasource>.password`             | The database connection password |
| `<datasource>.catalog`              | The catalog in the database. For SQL Server use the "database name" (such as `MY_DB`); for PostgreSQL leave empty. This value is case-sensitive |
| `<datasource>.schema`               | The schema name. This value is case-sensitive |
| `<datasource>.table.filter`         | Optional. A list of comma-separated table names that will be included in the hash. Tables not mentioned in this list will be excluded from the hashing and validation. This value is case-insensitive |
| `<datasource>.remove.table.prefix`  | Optional. A prefix to be removed from the table names. If your table "client" was renamed as "old_client" and you still want it to be recorded in the hash file as "client", then specify "old_" in this property. This value is case-insensitive
| `<datasource>.column.filter`        | Optional. A list of comma-separated column names that will be included in the hash. Columns not mentioned in this list will be excluded from the hashing and validation. This value is case-insensitive |
| `<datasource>.max.rows`             | Optional. Limits the maximum number of rows per table to be hashed. Could be useful for test runs of for debugging if you want to work with a small and fast data set |
| `<datasource>.select.autocommit`    | Optional. Overrides the default autocommit mode for SELECT queries. PostgreSQL defaults to `false` while the other databases default to `true`. This autocommit mode is used to run large SELECT queries. For more details see the section **Autocommit While Reading** below |
| `<datasource>.select.fetch.size`    | Optional. Used in conjunction with `<datasource>.select.autocommit` to enable buffering in SELECTs in some databases. If not specified defaults to 100 |
| `<datasource>.type.rules`           | Optional. Rules to override the default Java types used for each database type; it's a semicolon-separated list of rules. For more details see the section **Type Rules** below |
| `<datasource>.hashing.ordering` | Optional. Declares hashing ordering. It overrides the primary key ordering for tables with primary keys, and declares a specific ordering for tables with no primary keys. It takes the form of a semicolon-separated list of table sorting rules. For more details see the section **Hashing Ordering** below |
| `<datasource>.hashing.collation`    | Optional. Specifies the collation for the VARCHAR/CHAR columns used in the sorting ordering when hashing the data of the tables. This is particularly useful when two databases use different collations and sort rows in different order by default. If specified, this collation will be applied to all VARCHAR/CHAR columns in the ORDER BY clause, notwithstanding they belong to the primary key or other index/constraint on the table. This is only used for sorting purposes, not for converting/massaging data before hashing |
| `<datasource>.readonly` | Optional. Declares this datasource as readonly (default) or writable. This property is  a safeguard to protect the datasources when copying data. A destination datasouce needs to be explicitly set as writable (`readonly=false`) for the `copy` command to work |
| `<datasource>.insert.batch.size` | Optional. Declares the insert batch size when copying data from one database to another. Defaults to 100 |
| `<datasource>.log.sql`              | Optional. Defaults to `false`. Log the SQL queries executed in the datasource |

### Step 3 - Commands

HighFive implements essential commands that will be used for the most typical uses cases and auditing commands used to unravel and find a remedy to special cases when the migrated data does not fully match in two or more databases.

#### Essential Commands

| Command | Description |
| --  | -- |
| `listtables <datasource>` | Connects to the schema, list the tables in it, and checks they are all supported. Only tables and columns selected by the filters are considered. Useful to validate the connection and basic functionality |
| `listcolumns <datasource>` | Connects to the schema, list the tables and their columns in it and verify they are all supported. Only tables and columns selected by the filters are considered |
| `hash <datasource>` | Hashes the schema and saves the result to the file `<datasource>.hash` |
| `verify <datasource> <baseline-file>` | Hashes the schema and saves the result to the file `<datasource>.hash`. It then compares the computed hashed results with the *baseline-file* to decide if the comparison succeeds or fails |
| `copy <from-datasource> <to-datasource>` | Copies the data of the tables from a source datasource to a destination datasource. The destination tables must be empty. The destination datasource should not be readonly; that is, the property `<datasource>.readonly` should be explicitly set to `false`. The java types of the columns of the selected tables must match, even if the database types are different; use the `<datasource>.type.rules` to set java types explicitly. All database constraints and database auto-generated features should be disabled (or dropped) while the data is being copied |

#### Auditing Commands

| Command | Description |
| --  | -- |
| `hashd <datasource> <table> [<start> <end> [<step>]]` | The Hash Dump command dumps row hashes for a single table to the file `<datasource>.dump`. If `start` and `end` are specified, it only dumps the specific row range of the table. If the `step` value is also specified it saves one hash every this number of rows (to reduce the size of the dump file) |
| `hashc <datasource> <table> <dump-file>` | The Hash Compare command compares the a table against the baseline dump file produced by the `hashd` command. If it finds different hash values for a row, it displays the hashes, the row number, and then stops. It automatically detects the dump file range and step, if present, and acts accordingly |
| `hashl <datasource> <table> <start> <end>` | The Hash Log command displays the hash value for each field of each row of a table. Very verbose. Can be used to find out why two seemingly identical tables in two databases are actually producing different hashes. Only the selected row range is displayed, although all previous rows are computed |

There can be many issues that can cause the migrated data to not match the source data for a table. To name a few, consider:

- Mismatching collations in VARCHAR columns that can silently transform characters when they cannot be represented in the destination database.
- Time zones that cannot represent certain times of the some days, due to daylight savings time switching. For example, some databases allow any TIMESTAMP while other cannot represent March 10, 2024 at 2:15 am in the America/New York time zone; the clock jumped from 1:59 am to 3:00 am that night.
- Mismatching collations can sort alphabetic and non-alphabetic characters in very different ways in each database; the sorting order is crucial to correctly compute the hashes in each table.
- Some databases are permissive when it comes to trailing spaces in foreign keys, while other require exact matches. An effort to fix those and trim them will necessarily produce a mismatch when comparing data.

#### General Strategy To Resolve Mismatching Data In Tables

If you think a table was copied correcly but the hashing verification still show differences, then the following strategy will allow you to find the root cause of the issue.

##### 1. Find The Mismatching Rows

First, use the `hashd` command ("hash dump") on the specific table to generated a dump file with hashes for each row of the table. You now have a dump file, ready to be compared to the table in the other database.

Second, use the `hashc` command ("hash compare") to compare the table in the other database to the dump file you generated in the previous step. This command will compare the hashes row-by-row and will inform you if all the hashes fully match or not. If the don't it will inform the specific row where the difference was found.

Finally, use the `hashl` command ("hash log") in both databases to display all the values for the fields in the specific rows and how the hash is being computed for each field. Once you find out on which field the hash becomes different, then the field values are different. Sometimes the difference is apparent (a trailing space), sometimes the differences are not easy to spot (an extra decimal place in a timestamp with fractional seconds, or a unexpected collation enconding).

**Note**: If the table is too large you can use the `step` parameter of the `hashd` command to generate one hash per thousand rows or so, to keep the dump file size manageable. The `start` and `end` parameters can also help to narrow down of rows you are inspecting.

**Note**: The concept of first row, second row is artificial since relational databases tables do not have inherent row ordering. In this case the row numbering is done according to the hashing ordering specified in the datasource configuration, either using the primary key of the table or explicitly names ordering columns; this ordering can be further affected by the specified collations on VARCHAR columns.

##### 2. Fix The Mismatching Row

Once the data mismatch is found, then there are two main outcomes: one decide that the difference is "explained" and no further action is needed, or take actions to make both databases exactly equal.

If the latter option is decided, then the typical effort is done in the destination databases, the new one. Most of the time the new database can be easily "fixed" by extra SQL script that massages the data to fix the special cases.

However, sometimes this is not possible: think of a foreign key value that needs to be trimmed to actually work in the new database. In cases like these, it's typically the source data that has an anomaly that is rejected by the destination database. If this is the case, maybe it's possible to fix the data in the source; this should be carefully assessed with enough time to test the source application and make any necessary adjustments.

Whichever option is decided, once the data is fixed the table needs to be migrated again and also verified to make sure it fully matches.


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
2024-08-12 11:54:32.612 INFO  -   client: 101,782 rows
2024-08-12 11:54:32.615 INFO  -   invoice: 25,668 rows
2024-08-12 11:54:32.615 INFO  -   payment: 22,018 rows
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
2024-07-31 09:37:46.727 INFO  -    client: 101,782 rows
2024-07-31 09:37:46.727 INFO  -    invoice: 25,668 rows
2024-07-31 09:37:46.727 INFO  -    payment: 22,018 rows
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
2024-07-31 09:42:44.633 INFO  -   101,782 row(s) read
2024-07-31 09:42:44.670 INFO  - Reading table: invoice
2024-07-31 09:42:44.737 INFO  -   25,668 row(s) read
2024-07-31 09:42:44.811 INFO  - Reading table: payment
2024-07-31 09:42:44.813 INFO  -   22,018 row(s) read
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
2024-07-31 09:46:18.725 INFO  -   101,782 row(s) read
2024-07-31 09:46:18.732 INFO  - Reading table: invoice
2024-07-31 09:46:18.738 INFO  -   25,668 row(s) read
2024-07-31 09:46:18.745 INFO  - Reading table: payment
2024-07-31 09:46:18.748 INFO  -   22,018 row(s) read
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
2024-08-13 11:01:00.968 INFO  -   101,782 row(s) copied
2024-08-13 11:01:00.969 INFO  - Copying table invoice:
2024-08-13 11:01:01.001 INFO  -   25,668 row(s) copied
2024-08-13 11:01:01.002 INFO  - Copying table payment:
2024-08-13 11:01:02.063 INFO  -   22,018 row(s) copied
2024-08-13 11:01:02.063 INFO  - Copy complete -- Grand total of 149,468 row(s) copied
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

In any application, there are multiple ways of reading a column of a table. For example, a `DECIMAL(4, 0)` could be read in Java as a `short`, as an `int`, or even as a `long`. When it comes to hashing a value it's important to use the exact same Java representation across the board, for all related
databases; otherwise the same value could produce a different hash if misread as a `short`, `int`,
or `long`; this would defeat the purpose of the hash comparison.

In cases when the default java types end up being different between databases, it's possible to
enforce them per database by using the property `<datasource>.type.rules`. Any declared rule in
this property supersedes the default ones.

One or more rules can be declared for a datasource by specifying a semicolon-separated list of rules. Also, rules are case-insensitive when typing them.

You can use the command `listcolumns` to display and verify the *effective* java type for a column before/after you declare type rules.

#### Available Java Types

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

Most of the time the java types automatically selected by this tool are adequate and stable for hashing. However, there are some exceptions, particularly for database or database types that are old or badly defined.

#### Examples

For example, the `DATE` type in the Oracle database seems to imply that it stores dates without time, but in fact it stores both. By default, this tool reads this data as `LocalDateTime` to preserve the time component of it. However, when the data is migrated maybe only the date part ends up making it through to the other database; for verification purposes that would require you to compare them as plain dates. Therefore, if you wanted to read `DATE` columns just as `LocalDate` (discarding the time component) you could do so by adding a rule in the form `<database-type>:<java-type>`; in this case this rule can look like:

```bash
  <datasource>.type.rules=date:localdate
```


As an additional example, if you wanted to read all `DATE` columnas as `localdate` (discarding the
time component) and all `NUMBER(9, 0)` as `integer` values (assuming none exceeds 2^63-1) you could define the rule:

```bash
  <datasource>.type.rules=date:localdate;number(9, 0):integer
```


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

#### Example

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




