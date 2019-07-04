package com.panxt.gmall0105.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author panxt
 * @create 2019-06-28 8:53
 */
public interface OrderMapper {


    //1 查询当日交易额总数

    public Double selectOrderAmountTotal(String date);


    //2 查询当日交易额分时明细
    public List<Map> selectOrderAmountHourMap(String date);
}
