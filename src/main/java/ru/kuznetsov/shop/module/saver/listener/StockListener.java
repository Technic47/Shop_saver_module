package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.StockService;
import ru.kuznetsov.shop.module.saver.service.DefaultWorkFlowService;
import ru.kuznetsov.shop.represent.dto.StockDto;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;

@Component
@RequiredArgsConstructor
public class StockListener {

    private final StockService stockService;
    private final DefaultWorkFlowService defaultWorkFlowService;

    Logger logger = LoggerFactory.getLogger(StockListener.class);

    @KafkaListener(topics = STOCK_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String stockDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        defaultWorkFlowService.save(
                stockDto,
                operationId,
                logger,
                stockService,
                StockDto.class,
                STOCK_SAVE_SUCCESSFUL_TOPIC,
                STOCK_SAVE_FAIL_TOPIC);
    }
}
