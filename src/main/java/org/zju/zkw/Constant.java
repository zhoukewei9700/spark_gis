package org.zju.zkw;

import java.util.HashMap;

public class Constant {
    public static String EXTENT_SEPORATOR = " ";
    public static int BAND_NUM = 1;

    public static String TIF_FOLDER = "/root/zkw/testpic";
    public static String SUB_FOLDER = "/root/zkw/OutImg";
    public static String PNG_FOLDER = "/root/zkw/OutPng";
    public static String JSON_PATH = "/root/zkw/mbtiles";

    public static String TILE_DIR = "/root/zkw/OutPng";

    public static String MBTILES_DIR = "/root/zkw/mbtiles/testV2.mbtiles";

    public static String MBUTIL_DIR = "/root/zkw/mbutil-master";

    public static HashMap<Integer, Double> zResolution = new HashMap<Integer, Double>() {
        {
            put(0, 156543.033928);
            put(1, 78271.516964);
            put(2, 39135.758482);
            put(3, 19567.879241);
            put(4, 9783.939621);
            put(5, 4891.969810);
            put(6, 2445.984905);
            put(7, 1222.992453);
            put(8, 611.496226);
            put(9, 305.748113);
            put(10, 152.874057);
            put(11, 76.437028);
            put(12, 38.218514);
            put(13, 19.109257);
            put(14, 9.554629);
            put(15, 4.777314);
        }
    };

}
