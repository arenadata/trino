/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogManagerConfig;
import io.trino.connector.MockConnectorPlugin;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogInfo;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.testing.TestingSession.testSession;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestArenadataCatalogManager
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final String TEST_CATALOG2 = "test_catalog1";
    private static final String TEST_CATALOG3 = "test_catalog2";
    private static final ImmutableList<Property> TPCH_PROPERTIES =
            ImmutableList.of(new Property(new Identifier("tpch.partitioning-enabled"),
                    new StringLiteral("false")));
    private static final String SYSTEM_CATALOG = "system";
    private static final String POSTGRES_VERSION = "postgres:12";

    protected QueryRunner queryRunner;
    private CreateCatalogTask createTask;
    private DropCatalogTask dropTask;
    private PostgreSQLContainer<?> dockerContainer;

    @BeforeAll
    public void setUp()
    {
        String user = "postgres";
        String password = "postgres";
        String database = "postgres";
        dockerContainer = new PostgreSQLContainer<>(POSTGRES_VERSION)
                .withDatabaseName(database)
                .withUsername(user)
                .withPassword(password)
                .withCommand("postgres", "-c", "log_destination=stderr", "-c", "log_statement=all");
        dockerContainer.start();

        QueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION, builder -> {
            builder.setCatalogMangerKind(CatalogManagerConfig.CatalogMangerKind.ARENADATA);
            builder.setProperties(Map.of(
                    "arenadata.catalog.store.jdbc.url", dockerContainer.getJdbcUrl(),
                    "arenadata.catalog.store.jdbc.username", user,
                    "arenadata.catalog.store.jdbc.password", password));
        });
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.installPlugin(new MockConnectorPlugin(new FailConnectorFactory()));
        Map<Class<? extends Statement>, DataDefinitionTask<?>> tasks = queryRunner.getCoordinator().getInstance(new Key<>()
        {
        });
        createTask = (CreateCatalogTask) tasks.get(CreateCatalog.class);
        dropTask = (DropCatalogTask) tasks.get(DropCatalog.class);
        this.queryRunner = queryRunner;
    }

    @AfterAll
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        queryRunner = null;
        dockerContainer.stop();
    }

    @AfterEach
    public void afterDown()
    {
        DropCatalog statement = new DropCatalog(new Identifier(TEST_CATALOG), true, false);
        DropCatalog statement2 = new DropCatalog(new Identifier(TEST_CATALOG2), true, false);
        DropCatalog statement3 = new DropCatalog(new Identifier(TEST_CATALOG3), true, false);
        getFutureValue(dropTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        getFutureValue(dropTask.execute(statement2, createNewQuery(), emptyList(), WarningCollector.NOOP));
        getFutureValue(dropTask.execute(statement3, createNewQuery(), emptyList(), WarningCollector.NOOP));
    }

    @Test
    public void testDuplicatedCreateCatalog()
    {
        CreateCatalog statement = new CreateCatalog(new Identifier(TEST_CATALOG), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(createTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isTrue();
        assertThat(queryRunner.getPlannerContext().getMetadata().listCatalogs(createNewQuery().getSession()).stream()
                .map(CatalogInfo::catalogName)
                .collect(Collectors.toSet())).isEqualTo(Set.of(SYSTEM_CATALOG, TEST_CATALOG));
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(createTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP)))
                .withMessage("Catalog '%s' already exists", TEST_CATALOG);
    }

    @Test
    public void testDuplicatedCreateCatalogIfNotExists()
    {
        CreateCatalog statement = new CreateCatalog(new Identifier(TEST_CATALOG), true, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(createTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isTrue();
        getFutureValue(createTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isTrue();
        assertThat(queryRunner.getPlannerContext().getMetadata().listCatalogs(createNewQuery().getSession()).stream()
                .map(CatalogInfo::catalogName)
                .collect(Collectors.toSet())).isEqualTo(Set.of(SYSTEM_CATALOG, TEST_CATALOG));
    }

    @Test
    public void testDropNotExistedCatalog()
    {
        queryRunner.createCatalog(TEST_CATALOG, "tpch", ImmutableMap.of());
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isTrue();
        DropCatalog statement = new DropCatalog(new Identifier(TEST_CATALOG), false, false);
        getFutureValue(dropTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isFalse();
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(dropTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP)))
                .withMessage("Catalog '%s' not found", TEST_CATALOG);
        assertThat(queryRunner.getPlannerContext().getMetadata().listCatalogs(createNewQuery().getSession()).stream()
                .map(CatalogInfo::catalogName)
                .collect(Collectors.toSet())).isEqualTo(Set.of(SYSTEM_CATALOG));
    }

    @Test
    public void testDropCatalogIfNotExists()
    {
        queryRunner.createCatalog(TEST_CATALOG, "tpch", ImmutableMap.of());
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isTrue();
        DropCatalog statement = new DropCatalog(new Identifier(TEST_CATALOG), true, false);
        getFutureValue(dropTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isFalse();
        getFutureValue(dropTask.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isFalse();
        assertThat(queryRunner.getPlannerContext().getMetadata().listCatalogs(createNewQuery().getSession()).stream()
                .map(CatalogInfo::catalogName)
                .collect(Collectors.toSet())).isEqualTo(Set.of(SYSTEM_CATALOG));
    }

    @Test
    public void testCreateCatalogs()
    {
        CreateCatalog statement1 = new CreateCatalog(new Identifier(TEST_CATALOG), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        CreateCatalog statement2 = new CreateCatalog(new Identifier(TEST_CATALOG2), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        CreateCatalog statement3 = new CreateCatalog(new Identifier(TEST_CATALOG3), false, new Identifier("tpch"), TPCH_PROPERTIES, Optional.empty(), Optional.empty());
        getFutureValue(createTask.execute(statement1, createNewQuery(), emptyList(), WarningCollector.NOOP));
        getFutureValue(createTask.execute(statement2, createNewQuery(), emptyList(), WarningCollector.NOOP));
        getFutureValue(createTask.execute(statement3, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG)).isTrue();
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG2)).isTrue();
        assertThat(queryRunner.getPlannerContext().getMetadata().catalogExists(createNewQuery().getSession(), TEST_CATALOG3)).isTrue();
        assertThat(queryRunner.getPlannerContext().getMetadata().listCatalogs(createNewQuery().getSession()).stream()
                .map(CatalogInfo::catalogName)
                .collect(Collectors.toSet())).isEqualTo(Set.of(SYSTEM_CATALOG, TEST_CATALOG, TEST_CATALOG2, TEST_CATALOG3));
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(() -> getFutureValue(createTask.execute(statement1, createNewQuery(), emptyList(), WarningCollector.NOOP)))
                .withMessage("Catalog '%s' already exists", TEST_CATALOG);
    }

    @Test
    public void failCreateCatalog()
    {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getFutureValue(createTask.execute(
                        new CreateCatalog(
                                new Identifier(TEST_CATALOG),
                                true,
                                new Identifier("fail"),
                                ImmutableList.of(),
                                Optional.empty(),
                                Optional.empty()),
                        createNewQuery(),
                        emptyList(),
                        WarningCollector.NOOP)))
                .withMessageContaining("TEST create catalog fail: " + TEST_CATALOG);
    }

    private QueryStateMachine createNewQuery()
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                testSession(queryRunner.getDefaultSession()),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                queryRunner.getTransactionManager(),
                queryRunner.getAccessControl(),
                directExecutor(),
                queryRunner.getPlannerContext().getMetadata(),
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                Optional.empty(),
                true,
                new NodeVersion("test"));
    }

    private static class FailConnectorFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return "fail";
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            throw new IllegalArgumentException("TEST create catalog fail: " + catalogName);
        }
    }
}
