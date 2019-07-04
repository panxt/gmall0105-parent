package com.panxt.gmall0105.canal.app;


/**
 * @author panxt
 * @create 2019-06-28 17:51
 */
public class CanalMain {
    /*public static void main(String[] args) {
        CanalClient.watch("hadoop103",
                11111,
                "example",
                "gmall0105.order_info");
                }*/

    public static void main(String[] args) {
        CanalClient.watch("hadoop103",
                11111,
                "example",
                "gmall0105.*");
    }


}
