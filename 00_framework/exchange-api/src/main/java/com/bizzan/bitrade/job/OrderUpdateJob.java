package com.bizzan.bitrade.job;


import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.entity.ExchangeCoin;
import com.bizzan.bitrade.entity.ExchangeOrder;
import com.bizzan.bitrade.service.ExchangeCoinService;
import com.bizzan.bitrade.service.ExchangeOrderService;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class OrderUpdateJob {
    @Autowired
    private ExchangeOrderService orderService;
    @Autowired
    private ExchangeCoinService coinService;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // 5分钟检查一次超时订单
    @Scheduled(fixedRate = 300*1000)
    public void autoCancelOrder(){
        log.info("start autoCancelOrder...");
        List<ExchangeCoin> coinList = coinService.findAllEnabled();
        coinList.forEach(coin->{
            if(coin.getMaxTradingTime() > 0){
                List<ExchangeOrder> orders =  orderService.findOvertimeOrder(coin.getSymbol(), coin.getMaxTradingTime());
                orders.forEach(order -> {
                    // 发送消息至Exchange系统
                    kafkaTemplate.send("exchange-order-cancel", JSON.toJSONString(order));
                    log.info("orderId:"+order.getOrderId()+",time:"+order.getTime());
                });
            }
        });
        log.info("end autoCancelOrder...");
    }


}
