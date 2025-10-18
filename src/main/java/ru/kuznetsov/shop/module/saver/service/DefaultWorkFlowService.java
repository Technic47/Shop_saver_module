package ru.kuznetsov.shop.module.saver.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;
import ru.kuznetsov.shop.data.service.AbstractService;
import ru.kuznetsov.shop.data.service.KafkaService;
import ru.kuznetsov.shop.represent.dto.AbstractDto;

import java.util.Collections;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;

@Service
@RequiredArgsConstructor
public class DefaultWorkFlowService {

    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

    public <S extends AbstractService, T extends AbstractDto> void save(
            String itemJson,
            byte[] operationId,
            Logger logger,
            S service,
            Class<T> dtoClass,
            String successfulTopic,
            String failTopic) {

        String operationIdEncoded = new String(operationId);
        String dtoName = dtoClass.getSimpleName().toLowerCase().replace("dto", "");

        logger.info("Saving {} {} with operationId: {}", dtoName, itemJson, operationIdEncoded);

        try {
            T saved = (T) service.add(objectMapper.readValue(itemJson, dtoClass));
            kafkaService.sendMessageWithEntity(saved,
                    successfulTopic,
                    Collections.singletonMap(OPERATION_ID_HEADER, operationId));

            logger.info("Item {} saved. Id: {}, operationId: {}", dtoName, saved.getId(), operationIdEncoded);
        } catch (Exception e) {
            kafkaService.sendMessage(itemJson,
                    failTopic,
                    Collections.singletonMap(OPERATION_ID_HEADER, operationId));

            logger.error("Item {} saving failed. Product: {}, operationId: {}", dtoName, itemJson, operationIdEncoded);
            logger.error("Item {} saving failed.", dtoName, e);
        }
    }
}
