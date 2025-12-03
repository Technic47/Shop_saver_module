package ru.kuznetsov.shop.module.saver.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.kuznetsov.shop.data.service.AbstractService;
import ru.kuznetsov.shop.data.service.KafkaService;
import ru.kuznetsov.shop.represent.dto.AbstractDto;

import java.util.Collections;

import static ru.kuznetsov.shop.represent.common.KafkaConst.OPERATION_ID_HEADER;

@Service
@RequiredArgsConstructor
public class ListenerService {

    private final KafkaService kafkaService;
    private final ObjectMapper objectMapper;

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
            T saved = (T) service.add(objectMapper.readValue(itemJson, dtoClazz));
            kafkaService.sendMessageWithEntity(saved,
                    successfulTopic,
                    Collections.singletonMap(OPERATION_ID_HEADER, operationId));

            logger.info("Item {} saved. Id: {}, operationId: {}", dtoName, saved.getId(), operationIdEncoded);
        } catch (Exception e) {
            kafkaService.sendMessage(itemJson,
                    failTopic,
                    Collections.singletonMap(OPERATION_ID_HEADER, operationId));

            logger.error("Item saving failed. OperationId: {}, {}: {}, ", operationIdEncoded, dtoName, itemJson);
        }
    }
}
