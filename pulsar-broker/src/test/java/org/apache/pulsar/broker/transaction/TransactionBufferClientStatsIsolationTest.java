/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.transaction;

import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.util.HashedWheelTimer;
import io.opentelemetry.api.common.Attributes;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.broker.stats.OpenTelemetryTopicStats;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferClientImpl;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionBufferClientStatsIsolationTest extends TransactionTestBase {

    @BeforeClass
    protected void setup() throws Exception {
        // Keep the first broker alive while the test starts its own transaction coordinator.
        SharedPulsarCluster.get();
        setUpBase(1, 1, null, 0);
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testTopicMetricsWithAnotherBrokerRunning() throws Exception {
        String topic = BrokerTestUtil.newUniqueName("persistent://" + NAMESPACE1 + "/transaction-metrics");
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .sendTimeout(0, TimeUnit.SECONDS)
                .create();

        Transaction committed = pulsarClient.newTransaction().build().get();
        producer.newMessage(committed).value(new byte[]{1}).send();
        committed.commit().get();

        Transaction aborted = pulsarClient.newTransaction().build().get();
        producer.newMessage(aborted).value(new byte[]{2}).send();
        aborted.abort().get();

        var metrics = pulsarTestContexts.get(0).getOpenTelemetryMetricReader().collectAllMetrics();
        var attributes = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_TENANT, TENANT)
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, NAMESPACE1)
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topic)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.SUCCESS.attributes)
                .build();
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER,
                Attributes.builder().putAll(attributes)
                        .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes).build(), 1);
        assertMetricLongSumValue(metrics, OpenTelemetryTopicStats.TRANSACTION_BUFFER_CLIENT_OPERATION_COUNTER,
                Attributes.builder().putAll(attributes)
                        .putAll(OpenTelemetryAttributes.TransactionStatus.ABORTED.attributes).build(), 1);

        var collectorRegistry = pulsarTestContexts.get(0).getPrometheusMetricsRegistry();
        String[] labelNames = {"namespace", "topic"};
        String[] labelValues = {NAMESPACE1, topic};
        assertThat(collectorRegistry.getSampleValue(
                "pulsar_txn_tb_client_commit_latency_count", labelNames, labelValues)).isEqualTo(1D);
        assertThat(collectorRegistry.getSampleValue(
                "pulsar_txn_tb_client_abort_latency_count", labelNames, labelValues)).isEqualTo(1D);
    }

    @Test
    public void testFailedRegistrationReleasesClientResources() {
        var collectorRegistry = new CollectorRegistry();
        Counter.build("pulsar_txn_tb_client_commit_failed", "-").register(collectorRegistry);
        var timer = new HashedWheelTimer();
        try {
            assertThatThrownBy(() -> TransactionBufferClientImpl.create(
                    pulsarServiceList.get(0), timer, 1000, 3000, collectorRegistry))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(Collections.list(collectorRegistry.metricFamilySamples()))
                    .extracting(metric -> metric.name)
                    .containsExactly("pulsar_txn_tb_client_commit_failed");
            assertThatThrownBy(() -> timer.newTimeout(timeout -> { }, 1, TimeUnit.SECONDS))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            timer.stop();
        }
    }

    @Test
    public void testIndependentRegistriesCanBeReusedAfterClientCloses() throws Exception {
        var firstCollectorRegistry = new CollectorRegistry();
        var secondCollectorRegistry = new CollectorRegistry();
        @Cleanup
        TransactionBufferClient first = TransactionBufferClientImpl.create(
                pulsarServiceList.get(0), new HashedWheelTimer(), 1000, 3000, firstCollectorRegistry);
        @Cleanup
        TransactionBufferClient second = TransactionBufferClientImpl.create(
                pulsarServiceList.get(0), new HashedWheelTimer(), 1000, 3000, secondCollectorRegistry);

        assertThat(firstCollectorRegistry.getSampleValue("pulsar_txn_tb_client_pending_requests")).isZero();
        assertThat(secondCollectorRegistry.getSampleValue("pulsar_txn_tb_client_pending_requests")).isZero();
        first.close();
        first.close();
        assertThat(Collections.list(firstCollectorRegistry.metricFamilySamples())).isEmpty();
        assertThat(secondCollectorRegistry.getSampleValue("pulsar_txn_tb_client_pending_requests")).isZero();

        @Cleanup
        TransactionBufferClient replacement = TransactionBufferClientImpl.create(
                pulsarServiceList.get(0), new HashedWheelTimer(), 1000, 3000, firstCollectorRegistry);
        assertThat(firstCollectorRegistry.getSampleValue("pulsar_txn_tb_client_pending_requests")).isZero();
    }
}
