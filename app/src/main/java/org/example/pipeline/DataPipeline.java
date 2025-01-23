package org.example.pipeline;

import org.example.models.EnrichedSensorData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.List;

public class DataPipeline {
    private static final Logger log = LoggerFactory.getLogger(DataPipeline.class);

    public Flux<List<EnrichedSensorData>> startPipeline(int amount) {
        return DataProducer.produceData(amount)
                .transform(StageProcessors::filterData)
                .transform(StageProcessors::transformData)
                .transform(StageProcessors::enrichData)
                .transform(source -> StageProcessors.batchData(source, 5))
                .transform(StageProcessors::debounceData)
                .onErrorResume(e -> {
                    log.error("Error in pipeline: {}", e.getMessage());
                    return Flux.error(e);
                }).subscribeOn(Schedulers.boundedElastic());
    }
}
