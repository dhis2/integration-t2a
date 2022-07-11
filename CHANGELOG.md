# Changelog

## [1.0.0-RC3] - 11-07-2022

### Added
- Support [Personal Access Token](https://docs.dhis2.org/en/use/user-guides/dhis-core-version-master/working-with-your-account/personal-access-tokens.html) authentication
- Copy program indicator's aggregate export attribute option combo to data value's attribute option combo

### Fixed
- Wrong value is copied into the data value's category option combo

### Changed
- Rename `aggr.data.export.attr.id` parameter to `aggr.data.export.attr.id`

## [1.0.0-RC2] - 25-03-2022

### Added
- Run JAR as a shell executable on *nix operating systems

### Changed
- Default `http.endpoint.uri` parameter to `http://localhost:8081/dhis2/t2a`