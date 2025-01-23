package org.example.pipeline;

import com.google.gson.Gson;
import org.example.models.ErrorEvents;
import org.example.models.SensorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Random;


public class DataProducer {
    private static final Logger log = LoggerFactory.getLogger(DataProducer.class);
    private static final Gson gson = new Gson();
    private static final Random random = new Random();

    public static Flux<String> produceData(int amount) {
        return Flux.<String>create(emitter -> {
            for (int i = 0; i < amount; i++) {
                try {
                    Thread.sleep(random.nextLong(100, 1000));
                } catch (InterruptedException e) {
                    emitter.error(e);
                    return;
                }

                SensorData sensorData = new SensorData(
                        "sensor-" + random.nextInt(1, 5),
                        random.nextDouble(20.0, 30.0),
                        System.currentTimeMillis()
                );

                String data = gson.toJson(sensorData);
                if (random.nextFloat() < 0.1) {
                    log.error("Simulating error for: {}", data);
                    emitter.next(ErrorEvents.INVALID_DATA.name());
                } else {
                    log.info("Emitting: {}", data);
                    emitter.next(data);
                }
            }

            emitter.complete();
        }).subscribeOn(Schedulers.boundedElastic()); // Virtual threads for IO
    }

    public static Flux<String> withDelayAndError(Flux<String> source, long delay, double errorChance, String errorMessage) {
        return source.flatMap(value ->
                Flux.just(value)
                        .delayElements(Duration.ofMillis(delay))
                        .handle((item, sink) -> {
                            if (random.nextDouble() < errorChance) {
                                log.error("Simulate error: {}", errorMessage);
                                sink.error(new RuntimeException(errorMessage));
                            } else {
                                sink.next(item);
                            }
                        })
        );
    }
}
