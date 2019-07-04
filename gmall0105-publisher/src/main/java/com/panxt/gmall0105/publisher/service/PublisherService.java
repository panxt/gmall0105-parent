package com.panxt.gmall0105.publisher.service;

import java.util.Map;

/**
 * @author panxt
 * @create 2019-06-28 8:50
 */
public interface PublisherService {

    public Long getDauTotal(String date);

    public Map getDauHour(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);
}
