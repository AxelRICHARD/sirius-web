/*******************************************************************************
 * Copyright (c) 2022 Obeo.
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *     Obeo - initial API and implementation
 *******************************************************************************/
package org.eclipse.sirius.web.graphql.datafetchers.subscriptions;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

import org.eclipse.sirius.components.annotations.spring.graphql.SubscriptionDataFetcher;
import org.eclipse.sirius.components.collaborative.api.IEditingContextEventProcessorRegistry;
import org.eclipse.sirius.components.collaborative.formdescriptioneditors.api.FormDescriptionEditorConfiguration;
import org.eclipse.sirius.components.collaborative.formdescriptioneditors.api.IFormDescriptionEditorEventProcessor;
import org.eclipse.sirius.components.collaborative.formdescriptioneditors.dto.FormDescriptionEditorEventInput;
import org.eclipse.sirius.components.core.api.IPayload;
import org.eclipse.sirius.components.graphql.api.IDataFetcherWithFieldCoordinates;
import org.eclipse.sirius.web.graphql.schema.SubscriptionTypeProvider;
import org.reactivestreams.Publisher;

import graphql.schema.DataFetchingEnvironment;
import reactor.core.publisher.Flux;

/**
 * The data fetcher used to send the refreshed form description editor to a subscription.
 * <p>
 * It will be used to fetch the data for the following GraphQL field:
 * </p>
 *
 * <pre>
 * type Subscription {
 *   formDescriptionEditorEvent(input: FormDescriptionEditorEventInput): FormDescriptionEditorEventPayload
 * }
 * </pre>
 *
 * @author arichard
 */
@SubscriptionDataFetcher(type = SubscriptionTypeProvider.TYPE, field = SubscriptionFormDescriptionEditorEventDataFetcher.FORM_DESCRIPTION_EDITOR_EVENT_FIELD)
public class SubscriptionFormDescriptionEditorEventDataFetcher implements IDataFetcherWithFieldCoordinates<Publisher<IPayload>> {

    public static final String FORM_DESCRIPTION_EDITOR_EVENT_FIELD = "formDescriptionEditorEvent"; //$NON-NLS-1$

    private final ObjectMapper objectMapper;

    private final IEditingContextEventProcessorRegistry editingContextEventProcessorRegistry;

    public SubscriptionFormDescriptionEditorEventDataFetcher(ObjectMapper objectMapper, IEditingContextEventProcessorRegistry editingContextEventProcessorRegistry) {
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.editingContextEventProcessorRegistry = Objects.requireNonNull(editingContextEventProcessorRegistry);
    }

    @Override
    public Publisher<IPayload> get(DataFetchingEnvironment environment) throws Exception {
        Object argument = environment.getArgument(SubscriptionTypeProvider.INPUT_ARGUMENT);
        var input = this.objectMapper.convertValue(argument, FormDescriptionEditorEventInput.class);
        var formDescriptionEditorConfiguration = new FormDescriptionEditorConfiguration(input.getFormDescriptionEditorId());

        // @formatter:off
        return this.editingContextEventProcessorRegistry.getOrCreateEditingContextEventProcessor(input.getEditingContextId())
                .flatMap(processor -> processor.acquireRepresentationEventProcessor(IFormDescriptionEditorEventProcessor.class, formDescriptionEditorConfiguration, input))
                .map(representationEventProcessor -> representationEventProcessor.getOutputEvents(input))
                .orElse(Flux.empty());
        // @formatter:on
    }

}
