package com.kafka.cosumer.kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @Created with IDEA
 * @author:麻超
 * @Date:2019/12/22
 * @Time:11:58
 **/
@Component
@Slf4j
public class KafkaCoustomerListener {

    @KafkaListener(topics = "${certificate.info.topic}", id = "${kafka.consumer.group.id}",concurrency = "1", containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<?, ?>> records, Acknowledgment ack) {
        try {
            log.info("开始处理数据");
            List<String> messages = new ArrayList<>();
            for (ConsumerRecord<?, ?> record : records) {
                Optional<?> kafkaMessage = Optional.ofNullable(record.value());
                // 获取消息
                kafkaMessage.ifPresent(o -> {
                    try{
                        messages.add(o.toString());
                    }catch (Exception ex){
                        log.error("远程数据{},解析失败",o.toString());
                    }
                });
            }
            log.info("本次处理{}条数据",messages.size());
            if (messages.size() > 0) {
                System.out.println(messages.size());
            }
        } catch (Exception e) {
            log.error("本次监听Kafka数据失败",e);
        } finally {
            ack.acknowledge();
        }
    }

}

