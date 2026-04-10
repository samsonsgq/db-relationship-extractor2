package com.example.db2lineage.resolve;

import com.example.db2lineage.model.TargetObjectType;

import java.util.List;
import java.util.Optional;

public interface SchemaMetadataService {

    Optional<TargetObjectType> resolveObjectType(String objectName);

    List<String> listObjectColumns(String objectName);

    Optional<CallableSignature> resolveCallableSignature(String callableName);

    List<String> resolveTargetColumnListWhenSafelyKnown(String objectName);

    record CallableSignature(String callableName, List<String> argumentNames) {
        public CallableSignature {
            argumentNames = List.copyOf(argumentNames);
        }
    }
}
