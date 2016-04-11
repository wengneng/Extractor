package com.pactera.edg.am.metamanager.extractor.adapter.extract.cognos.bo;

public class Uuid {   
    protected static int count = 0;   
  
    public static synchronized String getUUID() {   
        count++;   
        long time = System.currentTimeMillis();   
  
        String timePattern = Long.toHexString(time);   
        int leftBit = 14 - timePattern.length();   
        if (leftBit > 0) {   
            timePattern = "0000000000".substring(0, leftBit) + timePattern;   
        }   
  
        String uuid = timePattern   
                + Long.toHexString(Double.doubleToLongBits(Math.random()))   
                + Long.toHexString(Double.doubleToLongBits(Math.random()))   
                + "000000000000000000";   
  
        uuid = uuid.substring(0, 32).toUpperCase();   
  
        return uuid;   
    }   
}  

