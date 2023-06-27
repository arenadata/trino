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
package io.trino.plugin.kinesis.s3config;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.kinesis.KinesisConnector;
import io.trino.plugin.kinesis.KinesisMetadata;
import io.trino.plugin.kinesis.KinesisPlugin;
import io.trino.plugin.kinesis.KinesisTableHandle;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.services.s3.S3Uri;

import java.net.URI;
import java.util.Map;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestS3TableConfigClient
{
    private static final Logger log = Logger.get(TestS3TableConfigClient.class);

    private final String tableDescriptionS3;
    private final String accessKey;
    private final String secretKey;

    public TestS3TableConfigClient()
    {
        tableDescriptionS3 = System.getProperty("kinesis.test-table-description-location");
        accessKey = System.getProperty("kinesis.awsAccessKey");
        secretKey = System.getProperty("kinesis.awsSecretKey");
    }

    @Test
    public void testS3URIValues()
    {
        // Verify that S3URI values will work:
        URI uri1 = URI.create("s3://our.data.warehouse/prod/client_actions");
        S3Uri s3Uri1 = S3Uri.builder().uri(uri1).build();
        assertThat(s3Uri1.key()).isNotNull();
        assertThat(s3Uri1.bucket()).isNotNull();

        assertThat(s3Uri1.toString()).isEqualTo("s3://our.data.warehouse/prod/client_actions");
        assertThat(s3Uri1.key()).hasValue("our.data.warehouse");
        assertThat(s3Uri1.bucket()).hasValue("prod/client_actions");
        assertThat(s3Uri1.region()).isNull();

        // show info:
        log.info("Tested out URI1 : %s", s3Uri1.toString());

        URI uri2 = URI.create("s3://some.big.bucket/long/complex/path");
        S3Uri s3Uri2 = S3Uri.builder().uri(uri2).build();
        assertThat(s3Uri2.key()).isNotNull();
        assertThat(s3Uri2.bucket()).isNotNull();

        assertThat(s3Uri2.toString()).isEqualTo("s3://some.big.bucket/long/complex/path");
        assertThat(s3Uri2.bucket()).hasValue("some.big.bucket");
        assertThat(s3Uri2.key()).hasValue("long/complex/path");
        assertThat(s3Uri2.region()).isNull();

        // info:
        log.info("Tested out URI2 : %s", uri2);

        URI uri3 = URI.create("s3://trino.kinesis.config/unit-test/trino-kinesis");
        S3Uri s3Uri3 = S3Uri.builder().uri(uri3).build();
        assertThat(s3Uri3.key()).isNotNull();
        assertThat(s3Uri3.bucket()).isNotNull();

        assertThat(s3Uri3.toString()).isEqualTo("s3://trino.kinesis.config/unit-test/trino-kinesis");
        assertThat(s3Uri3.bucket()).hasValue("trino.kinesis.config");
        assertThat(s3Uri3.key()).hasValue("unit-test/trino-kinesis");
    }

    @Test
    public void testTableReading()
    {
        // To run this test: setup an S3 bucket with a folder for unit testing, and put
        // MinimalTable.json in that folder.

        // Create dependent objects, including the minimal config needed for this test
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kinesis.table-description-location", tableDescriptionS3)
                .put("kinesis.default-schema", "kinesis")
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", TestUtils.noneToBlank(accessKey))
                .put("kinesis.secret-key", TestUtils.noneToBlank(secretKey))
                .put("bootstrap.quiet", "true")
                .buildOrThrow();

        KinesisPlugin kinesisPlugin = new KinesisPlugin();
        KinesisConnector kinesisConnector = TestUtils.createConnector(kinesisPlugin, properties, false);

        // Sleep for 10 seconds to ensure that we've loaded the tables:
        try {
            Thread.sleep(10000);
            log.info("done sleeping, will now try to read the tables.");
        }
        catch (InterruptedException e) {
            log.error("interrupted ...");
        }

        KinesisMetadata metadata = (KinesisMetadata) kinesisConnector.getMetadata(SESSION, new ConnectorTransactionHandle() {});
        SchemaTableName tblName = new SchemaTableName("default", "test123");
        KinesisTableHandle tableHandle = metadata.getTableHandle(SESSION, tblName);
        assertThat(metadata).isNotNull();
        SchemaTableName tableSchemaName = tableHandle.toSchemaTableName();
        assertThat(tableSchemaName.getSchemaName()).isEqualTo("default");
        assertThat(tableSchemaName.getTableName()).isEqualTo("test123");
        assertThat(tableHandle.getStreamName()).isEqualTo("test123");
        assertThat(tableHandle.getMessageDataFormat()).isEqualTo("json");
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle);
        assertThat(columnHandles.size()).isEqualTo(12);
    }
}
