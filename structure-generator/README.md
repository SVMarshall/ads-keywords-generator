# upload_structure

## Raison d'être:

TODO: your project description

## Features:

This project comes with number of preconfigured features, including:

### sbt-pack

Use `sbt-pack` instead of `sbt-assembly` to:
 * reduce build time
 * enable efficient dependency caching
 * reduce job submission time

To build package run:

```
sbt pack
```

### Testing

This template comes with an example of a test, to run tests:

```
sbt test
```

### Scala style

Find style configuration in `scalastyle-config.xml`. To enforce style run:

```
sbt scalastyle
```

### REPL

To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```
sbt repl/run
```

### Run sbt with BigQuery support

```
sbt -Dbigquery.project=adwords-dataflow
```

Note: To run with BQ support on IntelliJ, add JVM option in "Preferences > Build, execution, Deployment > sbt". Scio plugin also recommended.

---

This project is based on the [scio-template](https://github.com/spotify/scio-template).
