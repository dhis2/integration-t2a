# Tracker-to-Aggregate

image:https://github.com/dhis2/integration-t2a/workflows/CI/badge.svg[Build Status]

[Tracker-to-Aggregate](https://docs.dhis2.org/en/implement/maintenance-and-use/tracker-and-aggregate-data-integration.html#how-to-saving-aggregated-tracker-data-as-aggregate-data-values) (T2A) is a Java application that periodically aggregates and collects aggregate program indicators (PIs) from the DHIS2 server before pushing them back to the server as data values sets. T2A is a tool for DHIS2 administrators, designed to reduce the load of analytic operations on the DHIS2 server since requests for pre-aggregated data is often less demanding than on-the-fly aggregation of tracker data.

## Requirements

* JDK 11

## Getting Started

### Usage

```shell
java -jar dhis2-t2a.jar -Ddhis2.api.url=https://play.dhis2.org/2.37.2/api -Ddhis2.api.username=admin -Ddhis2.api.password=district -Dorg.unit.level=3 -Dperiods=2022Q1,2022Q2,2022Q3,2022Q4
```

### Config

By order of precedence, a config property can be specified:

1. as a Java System property
2. as an OS environment variable
3. in a key/value property file called `application.properties` or a YAML file named `application.yml`

| Config name           | Description                                                                                                                                                                                                       | Default Value | Example Value                       |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|-------------------------------------|
| `dhis2.api.url`       | DHIS2 server Web API URL.                                                                                                                                                                                         |               | `https://play.dhis2.org/2.37.2/api` |
| `dhis2.api.username`  | Username of the DHIS2 user to run as.                                                                                                                                                                             |               | `admin`                             |
| `dhis2.api.password`  | Password of the DHIS2 user to run as.                                                                                                                                                                             |               | `district`                          |
| `org.unit.level`      | Level of the organisation units to retrieve analytics for.                                                                                                                                                        |               | `3`                                 |
| `periods`             | [ISO or relative periods](https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-master/introduction.html#webapi_date_perid_format) to retrieve analytics for.                                         |               | `2022Q1,2022Q2,2022Q3,2022Q4`       |
| `pi.group.id`         | Program indicator group ID of the program indicators to retrieve analytics for.                                                                                                                                   |               | `Lesc1szBJGe`                       |
| `run.event.analytics` | Whether to generate event analytics before retrieving them.                                                                                                                                                       | `true`        | `false`                             |
| `thread.pool.size`    | Maximum no. of threads for processing analytics data. More threads might reduce execution time but also lead to more load on the DHIS2 server                                                                     | `1`           | `3`                                 |
| `split.org.units`     | Whether to process organisation units individually when retrieving analytics. It is more computationally expensive for the DHIS2 server to process organisation units in batches (i.e., `split.org.units=false`). | `true`        | `false`                             |
| `split.periods`       | Whether to process periods individually when retrieving analytics. It is more computationally expensive for the DHIS2 server to process periods in batches (i.e., `split.periods=false`).                         | `true`        | `false`                             |