# Changelog

## 1.10.0 (2026-08-28)

* Carried with the coordinated fleet release. No behaviour changed.

## 1.9.1 (2026-08-27)

* Carried with the coordinated fleet release. No adapter behaviour changed.

## 1.9.0 (2026-08-25)

* Delete is no longer falsely advertised for taskiq's standard read-only label source. Custom sources that implement `delete_schedule` retain the action through runtime capability detection.
* Reads and supported deletes for custom async schedule sources now execute on
  an explicit `source_loop`, rather than the z4j agent's background loop. The
  exact stock in-memory `LabelScheduleSource` keeps unbound read compatibility;
  other unbound or unavailable source loops fail closed without retry.
* Version bumped as part of the coordinated 1.9.0 fleet release, so every package in a deployment agrees on its peers.

## 1.8.0 (2026-07-23)

* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.7.0 (2026-07-07)

* SECURITY.md and README polished; capability documentation corrected.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: TaskIQ scheduler-source companion. Surfaces TaskIQ schedule sources in the dashboard.
