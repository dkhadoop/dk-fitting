package com.dksou.fitting.utils;

import sun.misc.BASE64Decoder;

public class Base64UtilsEN {

    public static byte[] decode(String base64) throws Exception {
        return new BASE64Decoder().decodeBuffer(base64);
    }
}