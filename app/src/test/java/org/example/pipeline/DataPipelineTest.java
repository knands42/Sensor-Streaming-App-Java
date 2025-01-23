package org.example.pipeline;

import org.example.models.EnrichedSensorData;
import org.example.models.ErrorEvents;
import org.example.models.SensorData;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

public class DataPipelineTest {
    @Test
    void testDataPipelineHappyPath() {
        DataPipeline pipeline = new DataPipeline();
        int amount = 5;
        StepVerifier.create(pipeline.startPipeline(amount))
                .expectNextCount(1)
                .expectComplete()
                .verify();
    }

    @Test
    void testDataPipelineWithErrors() {
        Flux<String> source = Flux.just("{\"sensorId\":\"sensor-1\",\"value\":25.0,\"timestamp\":1706803324997}", "error", "{\"sensorId\":\"sensor-1\",\"value\":25.0,\"timestamp\":1706803324997}");
        Flux<EnrichedSensorData> result = StageProcessors.enrichData(StageProcessors.transformData(StageProcessors.filterData(source)));
        StepVerifier.create(result)
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }

    @Test
    void testDataPipelineFilterData() {
        Flux<String> source = Flux.just("data1", ErrorEvents.INVALID_DATA.name(), "data2");
        Flux<String> filtered = StageProcessors.filterData(source);

        StepVerifier.create(filtered)
                .expectNext("data1", "data2")
                .expectComplete()
                .verify();
    }

    @Test
    void testDataPipelineTransformData() {
        Flux<String> source = Flux.just(
                "{\"sensorId\":\"sensor-1\",\"value\":25.0,\"timestamp\":1706803324997}",
                "invalid json",
                "{\"sensorId\":\"sensor-2\",\"value\":28.0,\"timestamp\":1706803324997}"
        );
        Flux<SensorData> transformed = StageProcessors.transformData(source);

        StepVerifier.create(transformed)
                .expectNextMatches(data -> data.sensorId().equals("sensor-1"))
                .expectNextMatches(data -> data.sensorId().equals("sensor-2"))
                .expectComplete()
                .verify();
    }

    @Test
    void testDataPipelineEnrichData() {
        SensorData sensor1 = new SensorData("sensor-1", 25.0, 1706803324997L);
        SensorData sensor2 = new SensorData("sensor-2", 28.0, 1706803324997L);
        Flux<SensorData> source = Flux.just(sensor1, sensor2);
        Flux<EnrichedSensorData> enriched = StageProcessors.enrichData(source);
        StepVerifier.create(enriched)
                .expectNextCount(2)
                .expectComplete()
                .verify();
    }

    @Test
    void testDataPipelineBatchData() {
        Flux<EnrichedSensorData> source = Flux.just(
                new EnrichedSensorData(new SensorData("sensor-1", 25.0, 1706803324997L), "location1"),
                new EnrichedSensorData(new SensorData("sensor-2", 26.0, 1706803324997L), "location2"),
                new EnrichedSensorData(new SensorData("sensor-3", 27.0, 1706803324997L), "location3"),
                new EnrichedSensorData(new SensorData("sensor-4", 28.0, 1706803324997L), "location4"),
                new EnrichedSensorData(new SensorData("sensor-5", 29.0, 1706803324997L), "location5")
        );
        int capacity = 2;
        Flux<List<EnrichedSensorData>> batched = StageProcessors.batchData(source, capacity);
        StepVerifier.create(batched)
                .expectNextMatches(list -> list.size() == 2)
                .expectNextMatches(list -> list.size() == 2)
                .expectNextMatches(list -> list.size() == 1)
                .expectComplete()
                .verify();
    }


    @Test
    void testDataPipelineDebounceData() throws InterruptedException {
        Flux<List<EnrichedSensorData>> source = Flux.concat(
                Flux.just(List.of(new EnrichedSensorData(new SensorData("sensor-1", 25.0, 1706803324997L), "location1"))).delayElements(Duration.ofMillis(200)),
                Flux.just(List.of(new EnrichedSensorData(new SensorData("sensor-2", 26.0, 1706803324997L), "location2"))).delayElements(Duration.ofMillis(100)),
                Flux.just(List.of(new EnrichedSensorData(new SensorData("sensor-3", 27.0, 1706803324997L), "location3"))).delayElements(Duration.ofMillis(600))
        );
        Flux<List<EnrichedSensorData>> debounced = StageProcessors.debounceData(source);
        StepVerifier.create(debounced)
                .expectNextCount(1)
                .expectNext();
    }

    @Test
    void testDataProducerProduceData() {
        int amount = 5;
        Flux<String> source = DataProducer.produceData(amount);
        StepVerifier.create(source)
                .expectNextCount(amount)
                .expectComplete()
                .verify();
    }

}
