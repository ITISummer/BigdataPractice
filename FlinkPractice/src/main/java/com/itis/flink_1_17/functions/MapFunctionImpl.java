package com.itis.flink_1_17.functions;

import com.itis.flink_1_17.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 */
public class MapFunctionImpl implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getId();
    }
}
