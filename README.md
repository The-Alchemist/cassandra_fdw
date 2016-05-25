cassandra_fdw
=============

Foreign Data Wrapper (FDW) that facilitates access to
[Cassandra](http://cassandra.apache.org/) 3.0+ from within
[PG](http://www.postgresql.org/) 9.5+.

## Install ##

This FDW is included in the [Postgres by BigSQL](http://bigsql.org)
distribution.

All you have to do is follow the usage instructions
below.  There are no special pre-requirements, since this *CassandraFDW*
is built with the native *cpp-driver*.

## Building from Source ##

In addition to normal PostgreSQL FDW pre-reqs, the primary specific
requirement for this FDW is the
[Cassandra CPP Driver](https://github.com/datastax/cpp-driver) *version
2.3*.

First, download the source code under the contrib subdirectory of the
PostgreSQL source tree and change into the FDW subdirectory:

```sh
cd cassandra_fdw
```

### Build and Install cpp-driver ###

Check out *version 2.3* of the cpp-driver:

```sh
git clone git@github.com:datastax/cpp-driver.git
cd cpp-driver
git checkout 2.3.0
```

Next, build and install it.  We show an example on Linux using
developer-defaults with all the transitive dependencies installed but
for more information please see the
[Build Documentation](http://datastax.github.io/cpp-driver/topics/building/)
from the cpp-driver project.

```sh
cmake .
make && sudo make install
```

### Build and Install the FDW ###

```sh
cd ..
USE_PGXS=1 make
USE_PGXS=1 make install
```

## Usage ##

The following parameter **must** be set on a Cassandra foreign server
object:

  * **`host`**: the address(es) or hostname(s) of the Cassandra server(s).
                Examples: "127.0.0.1", "127.0.0.1,127.0.0.2", "server1.domain.com".

The following parameters can be set on a Cassandra foreign table object:

  * **`schema_name`**: the name of the Cassandra KEYSPACE to query.
    Defaults to "public".

  * **`table_name`**: the name of the Cassandra TABLE to query.
    Defaults to the FOREIGN TABLE name used in the relevant CREATE command.

Here is an example:

```sql
-- Load EXTENSION first time after install.
CREATE EXTENSION cassandra_fdw;

-- CREATE SERVER object.
CREATE SERVER cass_serv FOREIGN DATA WRAPPER cassandra_fdw
    OPTIONS (host '127.0.0.1');

-- Create a USER MAPPING for the SERVER.
CREATE USER MAPPING FOR public SERVER cass_serv
    OPTIONS (username 'test', password 'test');

-- CREATE a FOREIGN TABLE.
--
-- Note that a valid "primary_key" OPTION is required in order to use
-- UPDATE or DELETE support.
CREATE FOREIGN TABLE test (id int) SERVER cass_serv
    OPTIONS (schema_name 'example', table_name 'oorder', primary_key 'id');

-- Query the FOREIGN TABLE.
SELECT * FROM test LIMIT 5;
```

For the full list of supported parameters, see [Reference Documentation PDF](doc.pdf).

###

Supports `IMPORT FOREIGN SCHEMA` feature

Here are some examples:

```sql
-- The Test_Tab1 was created as case sensitive in Cassandra and
-- test_tab2 was created as case-insensitive.  Only the tables
-- "Test_Tab1" and test_tab2 are imported from the Cassandra
-- TEST_SCHEMA keyspace.  If there are existing objects in the
-- PostgreSQL FOREIGN SCHEMA TEST_SCHEMA they will not be removed.
IMPORT FOREIGN SCHEMA TEST_SCHEMA
    LIMIT TO ("Test_Tab1", test_tab2)
    FROM SERVER cassandra_test_server INTO TEST_SCHEMA;

-- Import all other objects from the Cassandra TEST_SCHEMA schema
-- except "Test_Tab1" and test_tab2.
IMPORT FOREIGN SCHEMA TEST_SCHEMA
    EXCEPT ("Test_Tab1", test_tab2)
    FROM SERVER cassandra_test_server INTO TEST_SCHEMA;
```

Presently, `IMPORT`ing a `FOREIGN SCHEMA` does not automatically bring
in `PRIMARY KEY` information.  You can manually add the `OPTION`
*"primary_key"* to an *IMPORTed TABLE* using the `ALTER FOREIGN TABLE`
command as shown below:

```sql
ALTER FOREIGN TABLE test OPTIONS (ADD primary_key 'id');
```

## Documentation ##

[Reference PDF](doc.pdf)
