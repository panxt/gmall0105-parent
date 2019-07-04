package com.panxt.gmall0105.canal.app;

/**
 * @author panxt
 * @create 2019-07-02 19:05
 */
public class MyCanalMain {
    public static void main(String[] args) {
        MyCanalClient.watch("hadoop103",11111,"example","gmall0105.order_info");
    }
}
