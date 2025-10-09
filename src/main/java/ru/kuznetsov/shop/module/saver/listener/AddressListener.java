package ru.kuznetsov.shop.module.saver.listener;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.kuznetsov.shop.data.dto.AddressDto;
import ru.kuznetsov.shop.data.service.AddressService;

import static ru.kuznetsov.shop.data.common.KafkaTopics.ADDRESS_SAVE_TOPIC;

@Component
@RequiredArgsConstructor
public class AddressListener {

    private final AddressService addressService;

    Logger logger = LoggerFactory.getLogger(AddressListener.class);

    @KafkaListener(topics = ADDRESS_SAVE_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void saveProduct(AddressDto addressDto) {
        logger.info("Saving address {}", addressDto);
        addressService.add(addressDto);
    }
}
