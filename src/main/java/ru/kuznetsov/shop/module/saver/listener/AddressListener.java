package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.AddressService;
import ru.kuznetsov.shop.module.saver.service.DefaultWorkFlowService;
import ru.kuznetsov.shop.represent.dto.ProductDto;

import static ru.kuznetsov.shop.data.common.KafkaConst.ADDRESS_SAVE_TOPIC;
import static ru.kuznetsov.shop.data.common.KafkaConst.OPERATION_ID_HEADER;

@Component
@RequiredArgsConstructor
public class AddressListener {

    private final AddressService addressService;
    private final DefaultWorkFlowService defaultWorkFlowService;

    Logger logger = LoggerFactory.getLogger(AddressListener.class);

    @KafkaListener(topics = ADDRESS_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String addressDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        defaultWorkFlowService.save(
                addressDto,
                operationId,
                logger,
                addressService,
                ProductDto.class);
    }
}
