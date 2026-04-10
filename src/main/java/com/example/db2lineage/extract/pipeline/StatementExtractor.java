package com.example.db2lineage.extract.pipeline;

import com.example.db2lineage.extract.ExtractionContext;
import com.example.db2lineage.extract.RowCollector;
import com.example.db2lineage.parse.ParsedStatementResult;

public interface StatementExtractor {

    boolean supports(ParsedStatementResult parsedStatement);

    void extract(ParsedStatementResult parsedStatement, ExtractionContext context, RowCollector collector);
}
