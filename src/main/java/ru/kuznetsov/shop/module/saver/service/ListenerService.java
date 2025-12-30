package ru.kuznetsov.shop.module.saver.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.kuznetsov.shop.data.service.AbstractService;
import ru.kuznetsov.shop.kafka.service.KafkaService;
import ru.kuznetsov.shop.kafka.service.MessageCacheService;
import ru.kuznetsov.shop.represent.dto.AbstractDto;

import java.util.Collections;

import static ru.kuznetsov.shop.represent.common.KafkaConst.OPERATION_ID_HEADER;

@Service
@RequiredArgsConstructor
public class ListenerService {

    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;
    private final MessageCacheService<AbstractDto> messageCache;

    Logger logger = LoggerFactory.getLogger(ListenerService.class);

    public <S extends AbstractService, T extends AbstractDto> void save(
            String itemJson,
            byte[] operationId,
            S service,
            String successfulTopic,
            String failTopic,
            Class<T> dtoClazz) {

        String operationIdEncoded = new String(operationId);
        String dtoName = dtoClazz.getSimpleName()
                .toLowerCase()
                .replace("dto", "");

        logger.info("Saving {} {} with operationId: {}", dtoName, itemJson, operationIdEncoded);

        try {
            T item = objectMapper.readValue(itemJson, dtoClazz);

            if (!messageCache.exists(item)) {
                messageCache.put(item);

                T saved = (T) service.add(item);
                Long entityId = saved.getId();

                kafkaService.sendMessageWithEntity(saved,
                        successfulTopic,
                        Collections.singletonMap(OPERATION_ID_HEADER, operationId));

                logger.info("Item {} saved. Id: {}, operationId: {}", dtoName, entityId, operationIdEncoded);
            }
        } catch (Exception e) {
            kafkaService.sendMessage(itemJson,
                    failTopic,
                    Collections.singletonMap(OPERATION_ID_HEADER, operationId));

            logger.error("Item saving failed. OperationId: {}, {}: {}, ", operationIdEncoded, dtoName, itemJson);
        }
    }
}
