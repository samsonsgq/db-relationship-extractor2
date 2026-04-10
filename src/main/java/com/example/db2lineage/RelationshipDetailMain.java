package com.example.db2lineage;

import com.example.db2lineage.cli.CliArguments;
import com.example.db2lineage.cli.CliArgumentsParser;
import com.example.db2lineage.cli.CliMode;
import com.example.db2lineage.diff.RelationshipDetailTsvDiffer;
import com.example.db2lineage.diff.TsvDiffResult;
import com.example.db2lineage.emit.RelationshipDetailTsvWriter;
import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.pipeline.ExtractionPipeline;
import com.example.db2lineage.model.RelationshipRow;
import com.example.db2lineage.parse.ParsedStatementResult;
import com.example.db2lineage.parse.SqlSourceFile;
import com.example.db2lineage.parse.SqlSourceFileLoader;
import com.example.db2lineage.parse.SqlStatementParser;
import com.example.db2lineage.parse.StatementSlice;
import com.example.db2lineage.parse.StatementSlicer;
import com.example.db2lineage.resolve.InMemorySchemaMetadataService;
import com.example.db2lineage.validate.RelationshipDetailValidator;
import com.example.db2lineage.validate.ValidationIssue;
import com.example.db2lineage.validate.ValidationReport;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class RelationshipDetailMain {

    private RelationshipDetailMain() {
    }

    public static void main(String[] args) throws Exception {
        CliArgumentsParser parser = new CliArgumentsParser();
        CliArguments cliArguments;

        try {
            cliArguments = parser.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid CLI arguments: " + e.getMessage());
            System.exit(1);
            return;
        }

        logStartup(cliArguments);

        RunArtifacts run = runExtraction(cliArguments);
        Path outputFile = new RelationshipDetailTsvWriter().writeToOutputDir(cliArguments.outputDir(), run.rows());
        System.out.println("Wrote: " + outputFile);

        if (cliArguments.mode() == CliMode.GENERATE) {
            return;
        }

        ValidationReport validationReport = runValidation(cliArguments, run, outputFile);
        System.out.println(validationReport.toHumanReadable());

        if (cliArguments.mode() == CliMode.DIFF) {
            runDiff(cliArguments, outputFile);
        }

        if (cliArguments.mode() == CliMode.VALIDATE && cliArguments.failOnValidationError() && !validationReport.valid()) {
            System.exit(2);
        }
    }

    private static RunArtifacts runExtraction(CliArguments cliArguments) {
        List<SqlSourceFile> sourceFiles = new SqlSourceFileLoader().load(cliArguments);
        System.out.println("Discovered SQL source files: " + sourceFiles.size());

        StatementSlicer slicer = new StatementSlicer();
        SqlStatementParser sqlStatementParser = new SqlStatementParser();
        List<StatementSlice> allSlices = new ArrayList<>();
        List<ParsedStatementResult> parsedStatements = new ArrayList<>();

        for (SqlSourceFile sourceFile : sourceFiles) {
            List<StatementSlice> slices = slicer.slice(sourceFile);
            allSlices.addAll(slices);
            parsedStatements.addAll(sqlStatementParser.parseAll(slices));
        }

        long parsedCount = parsedStatements.stream().filter(result -> result.statement().isPresent()).count();
        long unsupportedCount = parsedStatements.stream().filter(ParsedStatementResult::unsupported).count();
        long parseFailedCount = parsedStatements.stream().filter(ParsedStatementResult::parseFailed).count();
        System.out.printf(
                "Parsed statement slices: total=%d, parsed=%d, unsupported=%d, parseFailed=%d%n",
                parsedStatements.size(),
                parsedCount,
                unsupportedCount,
                parseFailedCount
        );

        ExtractionContext extractionContext = new ExtractionContext(
                sourceFiles,
                InMemorySchemaMetadataService.fromParsedStatements(parsedStatements)
        );
        List<RelationshipRow> rows = new ExtractionPipeline().extract(extractionContext, parsedStatements);
        return new RunArtifacts(sourceFiles, allSlices, parsedStatements, rows);
    }

    private static ValidationReport runValidation(CliArguments cliArguments, RunArtifacts run, Path outputFile) throws Exception {
        RelationshipDetailValidator validator = new RelationshipDetailValidator();
        ValidationReport base = validator.validate(outputFile, run.sourceFiles());

        List<ValidationIssue> issues = new ArrayList<>(base.issues());

        if (run.sourceFiles().isEmpty()) {
            issues.add(new ValidationIssue("SKIPPED_INPUT_FILES", "No source files discovered from configured directories."));
        }

        if (run.slices().size() != run.parsedStatements().size()) {
            issues.add(new ValidationIssue("SKIPPED_STATEMENT_SLICES", "Mismatch between sliced statements and parsed results."));
        }

        RunArtifacts rerun = runExtraction(cliArguments);
        if (!run.rows().equals(rerun.rows())) {
            issues.add(new ValidationIssue("NON_DETERMINISTIC_OUTPUT", "Repeated runs on same input produced different rows."));
        }

        return new ValidationReport(issues.isEmpty(), base.rowCount(), issues);
    }

    private static void runDiff(CliArguments cliArguments, Path actualFile) {
        Path expectedDir = cliArguments.expectedOutputDir()
                .orElseThrow(() -> new IllegalArgumentException("--expectedOutputDir is required in diff mode."));
        Path expectedFile = expectedDir.resolve(RelationshipDetailTsvWriter.OUTPUT_FILE_NAME);

        TsvDiffResult diffResult = new RelationshipDetailTsvDiffer().diff(expectedFile, actualFile);
        System.out.println(diffResult.toHumanReadable());
    }

    private static void logStartup(CliArguments args) {
        System.out.println("Starting RelationshipDetailMain with directories:");
        System.out.println("  tableDir   : " + args.tableDir());
        System.out.println("  viewDir    : " + args.viewDir());
        System.out.println("  functionDir: " + args.functionDir());
        System.out.println("  spDir      : " + args.spDir());
        System.out.println("  outputDir  : " + args.outputDir());
        System.out.println("  mode       : " + args.mode());
        System.out.println("  failOnValidationError: " + args.failOnValidationError());
        args.extraDir().ifPresent(path -> System.out.println("  extraDir   : " + path));
        args.expectedOutputDir().ifPresent(path -> System.out.println("  expectedOutputDir: " + path));
    }

    private record RunArtifacts(
            List<SqlSourceFile> sourceFiles,
            List<StatementSlice> slices,
            List<ParsedStatementResult> parsedStatements,
            List<RelationshipRow> rows
    ) {
    }
}
