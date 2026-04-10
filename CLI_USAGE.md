# CLI usage (Phase 12)

The tool keeps the existing directory-based input model and always writes output to:

- `outputDir/relationship_detail.tsv`

## Generate only

```bash
java -cp target/db2-relationship-extractor-0.1.0-SNAPSHOT.jar com.example.db2lineage.RelationshipDetailMain \
  --tableDir <dir> --viewDir <dir> --functionDir <dir> --spDir <dir> --outputDir <dir> [--extraDir <dir>]
```

## Generate + validate

```bash
java ... com.example.db2lineage.RelationshipDetailMain \
  --tableDir <dir> --viewDir <dir> --functionDir <dir> --spDir <dir> --outputDir <dir> \
  --mode validate --failOnValidationError true [--extraDir <dir>]
```

## Generate + validate + diff against expected TSV

```bash
java ... com.example.db2lineage.RelationshipDetailMain \
  --tableDir <dir> --viewDir <dir> --functionDir <dir> --spDir <dir> --outputDir <dir> \
  --mode diff --expectedOutputDir <dir> [--extraDir <dir>]
```

In diff mode, `<expectedOutputDir>/relationship_detail.tsv` is compared with the generated file.
