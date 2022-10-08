/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.MLInputDataset;
import org.opensearch.ml.common.dataset.TextDocsInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.model.MLModelTaskType;
import org.opensearch.ml.common.output.model.ModelResultFilter;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;

/**
 * This class will act as an abstraction on the MLCommons client for accessing the ML Capabilities
 */
@RequiredArgsConstructor
@Log4j2
public class MLCommonsClientAccessor {
    private static final List<String> TARGET_RESPONSE_FILTERS = List.of("sentence_embedding");
    private final MachineLearningNodeClient mlClient;

    /**
     * Abstraction to call predict function of api of MLClient with default targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link MLModelTaskType#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a {@link List} of {@link List} of {@link Float} in the order of
     * inputText. We are not making this function generic enough to take any function or TaskType as currently we
     * need to run only TextEmbedding tasks only.
     *
     * @param modelId {@link String}
     * @param inputText {@link List} of {@link String} on which inference needs to happen
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out
     */
    public void inferenceSentences(
        @NonNull final String modelId,
        @NonNull final List<String> inputText,
        @NonNull final ActionListener<List<List<Float>>> listener
    ) {
        inferenceSentences(TARGET_RESPONSE_FILTERS, modelId, inputText, listener);
    }

    /**
     * Abstraction to call predict function of api of MLClient with provided targetResponse filters. It uses the
     * custom model provided as modelId and run the {@link MLModelTaskType#TEXT_EMBEDDING}. The return will be sent
     * using the actionListener which will have a {@link List} of {@link List} of {@link Float} in the order of
     * inputText. We are not making this function generic enough to take any function or TaskType as currently we
     * need to run only TextEmbedding tasks only.
     *
     * @param targetResponseFilters {@link List} of {@link String} which filters out the responses
     * @param modelId {@link String}
     * @param inputText {@link List} of {@link String} on which inference needs to happen
     * @param listener {@link ActionListener} which will be called when prediction is completed or errored out.
     */
    public void inferenceSentences(
        @NonNull final List<String> targetResponseFilters,
        @NonNull final String modelId,
        @NonNull final List<String> inputText,
        @NonNull final ActionListener<List<List<Float>>> listener
    ) {
        final ModelResultFilter modelResultFilter = new ModelResultFilter(false, true, targetResponseFilters, null);
        final MLInputDataset inputDataset = new TextDocsInputDataSet(inputText, modelResultFilter);
        final MLInput mlInput = new MLInput(FunctionName.CUSTOM, null, inputDataset, MLModelTaskType.TEXT_EMBEDDING);
        final List<List<Float>> vector = new ArrayList<>();

        mlClient.predict(modelId, mlInput, ActionListener.wrap(mlOutput -> {
            final ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlOutput;
            final List<ModelTensors> tensorOutputList = modelTensorOutput.getMlModelOutputs();
            for (final ModelTensors tensors : tensorOutputList) {
                final List<ModelTensor> tensorsList = tensors.getMlModelTensors();
                for (final ModelTensor tensor : tensorsList) {
                    vector.add(Arrays.stream(tensor.getData()).map(value -> (Float) value).collect(Collectors.toList()));
                }
            }
            log.debug("Inference Response for input sentence {} is : {} ", inputText, vector);
            listener.onResponse(vector);
        }, listener::onFailure));
    }

}
