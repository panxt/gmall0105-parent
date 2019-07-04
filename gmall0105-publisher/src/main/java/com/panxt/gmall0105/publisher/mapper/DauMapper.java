package com.panxt.gmall0105.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author panxt
 * @create 2019-06-28 8:53
 */
public interface DauMapper {


    //1.查询日活总数 com.panxt.gmall0105.publisher.mapper.DauMapper
    //select count(*) ct from gmall_0105_dau where logdate=date;
    public Long selectDauTotal(String date);

    //2.查询日活分时明细
    //select loghour,count(*) from gmall0105_dau where logdate=date group by loghour;
    public List<Map> selectDauHourMap(String date);
}
