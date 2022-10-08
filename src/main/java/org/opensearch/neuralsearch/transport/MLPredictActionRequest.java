/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.List;

import lombok.Getter;
import lombok.NonNull;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class MLPredictActionRequest extends ActionRequest {
    @Getter
    private final String modelId;

    @Getter
    private final List<String> inputSentencesList;

    public MLPredictActionRequest(final StreamInput in) throws IOException {
        super(in);
        this.modelId = in.readString();
        this.inputSentencesList = in.readStringList();
    }

    public MLPredictActionRequest(@NonNull final String modelID, @NonNull final List<String> inputText) {
        super();
        this.modelId = modelID;
        this.inputSentencesList = inputText;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(modelId);
        out.writeStringCollection(inputSentencesList);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (!Strings.hasText(modelId)) {
            return addValidationError("Model id cannot be empty ", null);
        }
        if (inputSentencesList.size() == 0) {
            return addValidationError("Input Sentences List cannot be empty ", null);
        }
        return null;
    }
}
