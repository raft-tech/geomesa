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
- **geomesa-index-api**: Indexing abstraction layer - defines how features are indexed and queried. Contains `GeoMesaDataStore` base class that all data stores extend.
- **geomesa-filter**: CQL filter parsing, DNF/CNF normalization, and spatial predicates
- **geomesa-features**: Feature encoding (Avro, Kryo serialization) and exporters
- **geomesa-z3**: Space-filling curve implementations (Z2, Z3, XZ2, XZ3, S2, S3) for spatio-temporal indexing
- **geomesa-utils-parent**: Common utilities, configuration, and BOM (Bill of Materials)

### Index Types
GeoMesa provides pluggable spatial/temporal indices (defined in `geomesa-index-api`):
- **Z3Index / XZ3Index**: Spatio-temporal (longitude, latitude, time) - use for time-series geo data
- **Z2Index / XZ2Index**: Spatial only (longitude, latitude) - use for static geo data
- **S2Index / S3Index**: Google S2-based spatial/spatio-temporal indexing
- **IdIndex**: Fast lookup by feature ID
- **AttributeIndex**: Secondary index on feature attributes

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
- **SimpleFeatureType (SFT)**: GeoTools schema definition for geospatial features (e.g., `"name:String,dtg:Date,*geom:Point:srid=4326"`)
- **Space-filling curves**: Z3/Z2 encode multi-dimensional coordinates into single sortable keys for efficient range queries
- **Iterators**: Server-side processing for Accumulo/HBase that push filtering to storage layer
- **Query Planning**: `FilterSplitter` and `QueryPlanner` in `geomesa-index-api` select optimal indices and strategies

## Testing

- **Unit tests**: `*Test.java` / `*Test.scala` - run with `mvn test`
- **Integration tests**: `*IT.java` / `*IT.scala` - run with `mvn failsafe:integration-test`
- **Framework**: Specs2 (Scala), JUnit 4/5
- **Docker required**: Tests use TestContainers for Accumulo, Cassandra, Kafka, HBase, PostgreSQL, Redis

## Code Style

- Scala code follows the [Scala style guide](https://docs.scala-lang.org/style/)
- License headers required on all source files (enforced by License Maven Plugin)
- PR titles: `GEOMESA-XXXX: Description` (reference JIRA ticket)
- Contributions require Eclipse CLA signature

## Key Dependencies

- GeoTools 34.1 (geospatial operations, DataStore interface)
- JTS 1.20.0 (geometry operations)
- Apache Spark 3.5.7
- Scala 2.12.20 (also supports 2.13.16)
