package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.StockService;
import ru.kuznetsov.shop.module.saver.service.ListenerService;
import ru.kuznetsov.shop.represent.dto.StockDto;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;

@Component
@RequiredArgsConstructor
public class StockListener {

    private final StockService stockService;
    private final ListenerService listenerService;

    @KafkaListener(topics = STOCK_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String stockDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        listenerService.save(
                stockDto,
                operationId,
                stockService,
                STOCK_SAVE_SUCCESSFUL_TOPIC,
                STOCK_SAVE_FAIL_TOPIC,
                StockDto.class);
    }
}
