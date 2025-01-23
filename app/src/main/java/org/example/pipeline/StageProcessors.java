package org.example.pipeline;

import com.google.gson.Gson;
import org.example.models.EnrichedSensorData;
import org.example.models.ErrorEvents;
import org.example.models.SensorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Random;

public class StageProcessors {
    private static final Logger log = LoggerFactory.getLogger(StageProcessors.class);
    private static final Gson gson = new Gson();
    private static final Random random = new Random();

    public static Flux<String> filterData(Flux<String> source) {
        return source.filter(it -> {
            if (it.equals(ErrorEvents.INVALID_DATA.name())) {
                log.warn("invalid data found, skipping");
                return false;
            } else {
                return true;
            }
        }).onErrorResume(e -> {
            log.error("Error filtering: {}", e.getMessage());
            return Flux.just("error");
        });
    }

    public static Flux<SensorData> transformData(Flux<String> source) {
        return source.flatMap(it ->
                Flux.defer(() -> {
                    log.info("Transforming data: {}", it);
                    try {
                        return Flux.just(gson.fromJson(it, SensorData.class));
                    } catch (Exception e) {
                        log.error("Error transforming: {}", e.getMessage());
                        return Flux.empty();
                    }
                }).subscribeOn(Schedulers.boundedElastic())
        );
    }

    public static Flux<EnrichedSensorData> enrichData(Flux<SensorData> source) {
        return source.flatMap(sensorData ->
                Flux.defer(() -> {
                    try {
                        Thread.sleep(random.nextLong(50, 500));
                    } catch (InterruptedException e) {
                        log.error("Error while sleeping: {}", e.getMessage());
                        return Flux.just(new EnrichedSensorData(sensorData, "Unknown Location"));
                    }

                    String location = "Location-" + random.nextInt(1, 10);
                    log.info("Enriching data: {}", sensorData);
                    return Flux.just(new EnrichedSensorData(sensorData, location));
                }).onErrorResume(e -> {
                    log.error("Error enriching: {}", e.getMessage());
                    return Flux.just(new EnrichedSensorData(sensorData, "Unknown Location"));
                })
        );
    }

    public static Flux<List<EnrichedSensorData>> batchData(Flux<EnrichedSensorData> source, int capacity) {
        return source.buffer(capacity);
    }

    public static Flux<List<EnrichedSensorData>> debounceData(Flux<List<EnrichedSensorData>> source) {
        return source.sample(Duration.ofMillis(500));
    }
}
