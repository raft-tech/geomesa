# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GeoMesa is an open source suite of tools for large-scale geospatial querying and analytics on distributed computing systems. It provides spatio-temporal indexing on top of Accumulo, HBase, Cassandra, Kafka (streaming), Redis, PostGIS, and file systems (Parquet/ORC).

## Build Commands

**Requirements:** JDK 17, Maven 3.8.1+, Docker (for tests)

```bash
# Full build (skip tests)
mvn clean install -Dmaven.test.skip

# Build with tests (requires Docker)
mvn clean install

# Faster parallel build
mvn clean install -T 1.5C -Dmaven.test.skip

# Build specific module
mvn clean install -pl geomesa-accumulo/geomesa-accumulo-datastore -am -Dmaven.test.skip

# Run unit tests for a module
mvn test -pl geomesa-accumulo/geomesa-accumulo-datastore

# Run a single test class
mvn test -pl geomesa-accumulo/geomesa-accumulo-datastore -Dtest=AccumuloDataStoreTest

# Run integration tests
mvn failsafe:integration-test -pl <module>

# Switch Scala version (default is 2.12)
./build/scripts/change-scala-version.sh 2.13
```

## Project Architecture

### Core Modules
- **geomesa-index-api**: Indexing abstraction layer - defines how features are indexed and queried
- **geomesa-filter**: CQL filter parsing and spatial predicates
- **geomesa-features**: Feature encoding (Avro, Kryo serialization)
- **geomesa-z3**: Z3 space-filling curve implementation for spatio-temporal indexing
- **geomesa-utils-parent**: Common utilities and base functionality

### Data Store Implementations
Each data store follows a similar structure with sub-modules:
- `*-datastore`: Core DataStore implementation (GeoTools DataStore interface)
- `*-tools`: Command-line tools
- `*-gs-plugin`: GeoServer plugin
- `*-dist`: Binary distribution assembly
- `*-spark`: Spark integration (where applicable)

Data stores: `geomesa-accumulo`, `geomesa-hbase`, `geomesa-cassandra`, `geomesa-kafka`, `geomesa-redis`, `geomesa-gt` (PostGIS), `geomesa-fs` (FileSystem)

### Processing Modules
- **geomesa-convert**: Data conversion framework with format-specific sub-modules (Avro, JSON, XML, Parquet, etc.)
- **geomesa-spark**: Apache Spark integration including PySpark and Spark SQL
- **geomesa-process**: WPS processes for GeoServer

### Key Concepts
- **SimpleFeatureType (SFT)**: GeoTools schema definition for geospatial features
- **Z3 Index**: 3D space-filling curve encoding longitude, latitude, and time into a single sortable key
- **Iterators**: Server-side processing for Accumulo/HBase that push filtering to storage layer

## Testing

- **Unit tests**: `*Test.java` / `*Test.scala` - run with `mvn test`
- **Integration tests**: `*IT.java` / `*IT.scala` - run with `mvn failsafe:integration-test`
- **Framework**: Specs2 (Scala), JUnit 4/5
- **Docker required**: Tests use TestContainers for Accumulo, Cassandra, Kafka, HBase, PostgreSQL, Redis

## Code Style

- Scala code follows the [Scala style guide](https://docs.scala-lang.org/style/)
- License headers required on all source files (enforced by License Maven Plugin)
- PR titles: `GEOMESA-XXXX: Description` (reference JIRA ticket)

## Key Dependencies

- GeoTools 34.1 (geospatial operations)
- JTS 1.20.0 (geometry operations)
- Apache Spark 3.5.7
- Scala 2.12.20 (also supports 2.13.16)
