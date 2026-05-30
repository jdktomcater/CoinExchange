package com.bizzan.bitrade.job;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.bizzan.bitrade.entity.CoinThumb;
import com.bizzan.bitrade.entity.ExchangeOrderDirection;
import com.bizzan.bitrade.entity.ExchangeTrade;
import com.bizzan.bitrade.entity.TradePlate;
import com.bizzan.bitrade.handler.NettyHandler;

import java.util.*;

@Component
public class ExchangePushJob {
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    @Autowired
    private NettyHandler nettyHandler;
    private Map<String,List<ExchangeTrade>> tradesQueue = new HashMap<>();
    private Map<String,List<TradePlate>> plateQueue = new HashMap<>();
    private Map<String,List<CoinThumb>> thumbQueue = new HashMap<>();

    public void addTrades(String symbol, List<ExchangeTrade> trades){
        List<ExchangeTrade> list = tradesQueue.computeIfAbsent(symbol, k -> new ArrayList<>());
        synchronized (list) {
            list.addAll(trades);
        }
    }

    public void addPlates(String symbol, TradePlate plate){
        List<TradePlate> list = plateQueue.computeIfAbsent(symbol, k -> new ArrayList<>());
        synchronized (list) {
            list.add(plate);
        }
    }

    public void addThumb(String symbol, CoinThumb thumb){
        List<CoinThumb> list = thumbQueue.computeIfAbsent(symbol, k -> new ArrayList<>());
        synchronized (list) {
            list.add(thumb);
        }
    }


    @Scheduled(fixedRate = 500)
    public void pushTrade(){
        for (Map.Entry<String, List<ExchangeTrade>> entry : tradesQueue.entrySet()) {
            String symbol = entry.getKey();
            List<ExchangeTrade> trades = entry.getValue();
            if (!trades.isEmpty()) {
                synchronized (trades) {
                    messagingTemplate.convertAndSend("/topic/market/trade/" + symbol, trades);
                    trades.clear();
                }
            }
        }
    }


    @Scheduled(fixedDelay = 2000)
    public void pushPlate(){
        for (Map.Entry<String, List<TradePlate>> entry : plateQueue.entrySet()) {
            String symbol = entry.getKey();
            List<TradePlate> plates = entry.getValue();
            if (!plates.isEmpty()) {
                boolean hasPushAskPlate = false;
                boolean hasPushBidPlate = false;
                synchronized (plates) {
                    for (TradePlate plate : plates) {
                        if (plate.getDirection() == ExchangeOrderDirection.BUY && !hasPushBidPlate) {
                            hasPushBidPlate = true;
                        } else if (plate.getDirection() == ExchangeOrderDirection.SELL && !hasPushAskPlate) {
                            hasPushAskPlate = true;
                        } else {
                            continue;
                        }
                        //websocket推送盘口信息
                        messagingTemplate.convertAndSend("/topic/market/trade-plate/" + symbol, plate.toJSON(24));
                        //websocket推送深度信息
                        messagingTemplate.convertAndSend("/topic/market/trade-depth/" + symbol, plate.toJSON(50));
                        //netty推送
                        nettyHandler.handlePlate(symbol, plate);
                    }
                    plates.clear();
                }
            }
        }
    }

    @Scheduled(fixedRate = 500)
    public void pushThumb(){
        for (Map.Entry<String, List<CoinThumb>> entry : thumbQueue.entrySet()) {
            String symbol = entry.getKey();
            List<CoinThumb> thumbs = entry.getValue();
            if (!thumbs.isEmpty()) {
                synchronized (thumbs) {
                    messagingTemplate.convertAndSend("/topic/market/thumb", thumbs.get(thumbs.size() - 1));
                    thumbs.clear();
                }
            }
        }
    }
}
