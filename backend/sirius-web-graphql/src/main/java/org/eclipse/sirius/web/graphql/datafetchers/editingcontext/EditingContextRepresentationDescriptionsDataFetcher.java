/*******************************************************************************
 * Copyright (c) 2019, 2022 Obeo.
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
package org.eclipse.sirius.web.graphql.datafetchers.editingcontext;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.eclipse.sirius.web.annotations.spring.graphql.QueryDataFetcher;
import org.eclipse.sirius.web.graphql.pagination.PageInfoWithCount;
import org.eclipse.sirius.web.graphql.schema.EditingContextTypeProvider;
import org.eclipse.sirius.web.representations.IRepresentationDescription;
import org.eclipse.sirius.web.spring.collaborative.api.IEditingContextEventProcessorRegistry;
import org.eclipse.sirius.web.spring.collaborative.dto.EditingContextRepresentationDescriptionsInput;
import org.eclipse.sirius.web.spring.collaborative.dto.EditingContextRepresentationDescriptionsPayload;
import org.eclipse.sirius.web.spring.graphql.api.IDataFetcherWithFieldCoordinates;

import graphql.relay.Connection;
import graphql.relay.ConnectionCursor;
import graphql.relay.DefaultConnection;
import graphql.relay.DefaultConnectionCursor;
import graphql.relay.DefaultEdge;
import graphql.relay.DefaultPageInfo;
import graphql.relay.Edge;
import graphql.relay.PageInfo;
import graphql.schema.DataFetchingEnvironment;
import reactor.core.publisher.Mono;

/**
 * The data fetcher used to retrieve the representation descriptions accessible to a viewer.
 * <p>
 * It will be used to fetch the data for the following GraphQL field:
 * </p>
 *
 * <pre>
 * type EditingContext {
 *   representationDescriptions(classId: ID): EditingContextRepresentationDescriptionConnection!
 * }
 * </pre>
 *
 * @author pcdavid
 * @author sbegaudeau
 */
@QueryDataFetcher(type = EditingContextTypeProvider.TYPE, field = EditingContextTypeProvider.REPRESENTATION_DESCRIPTIONS_FIELD)
public class EditingContextRepresentationDescriptionsDataFetcher implements IDataFetcherWithFieldCoordinates<CompletableFuture<Connection<IRepresentationDescription>>> {

    private static final String KIND_ARGUMENT = "kind"; //$NON-NLS-1$

    private final IEditingContextEventProcessorRegistry editingContextEventProcessorRegistry;

    public EditingContextRepresentationDescriptionsDataFetcher(IEditingContextEventProcessorRegistry editingContextEventProcessorRegistry) {
        this.editingContextEventProcessorRegistry = Objects.requireNonNull(editingContextEventProcessorRegistry);
    }

    @Override
    public CompletableFuture<Connection<IRepresentationDescription>> get(DataFetchingEnvironment environment) throws Exception {
        String editingContextId = environment.getSource();
        String kind = environment.getArgument(KIND_ARGUMENT);

        EditingContextRepresentationDescriptionsInput input = new EditingContextRepresentationDescriptionsInput(UUID.randomUUID(), editingContextId, kind);

        // @formatter:off
        return this.editingContextEventProcessorRegistry.dispatchEvent(input.getEditingContextId(), input)
                .filter(EditingContextRepresentationDescriptionsPayload.class::isInstance)
                .map(EditingContextRepresentationDescriptionsPayload.class::cast)
                .map(this::toConnection)
                .switchIfEmpty(Mono.just(new DefaultConnection<>(List.of(), new DefaultPageInfo(null, null, false, false))))
                .toFuture();
        // @formatter:on
    }

    private Connection<IRepresentationDescription> toConnection(EditingContextRepresentationDescriptionsPayload payload) {
        // @formatter:off
        List<Edge<IRepresentationDescription>> representationDescriptionEdges = payload.getRepresentationDescriptions().stream()
                .map(representationDescription -> {
                    String value = Base64.getEncoder().encodeToString(representationDescription.getId().toString().getBytes());
                    ConnectionCursor cursor = new DefaultConnectionCursor(value);
                    return new DefaultEdge<>(representationDescription, cursor);
                })
                .collect(Collectors.toList());
        // @formatter:on

        ConnectionCursor startCursor = representationDescriptionEdges.stream().findFirst().map(Edge::getCursor).orElse(null);
        ConnectionCursor endCursor = null;
        if (!representationDescriptionEdges.isEmpty()) {
            endCursor = representationDescriptionEdges.get(representationDescriptionEdges.size() - 1).getCursor();
        }
        PageInfo pageInfo = new PageInfoWithCount(startCursor, endCursor, false, false, payload.getRepresentationDescriptions().size());
        return new DefaultConnection<>(representationDescriptionEdges, pageInfo);
    }

}
