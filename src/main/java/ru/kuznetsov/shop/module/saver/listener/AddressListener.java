package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.service.AddressService;
import ru.kuznetsov.shop.module.saver.service.ListenerService;
import ru.kuznetsov.shop.represent.dto.AddressDto;

import static ru.kuznetsov.shop.represent.common.KafkaConst.*;

@Component
@RequiredArgsConstructor
public class AddressListener {

    private final AddressService addressService;
    private final ListenerService listenerService;

    @KafkaListener(topics = ADDRESS_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(String addressDto, @Header(OPERATION_ID_HEADER) byte[] operationId) {

        listenerService.save(
                addressDto,
                operationId,
                addressService,
                ADDRESS_SAVE_SUCCESSFUL_TOPIC,
                ADDRESS_SAVE_FAIL_TOPIC,
                AddressDto.class);
    }
}
