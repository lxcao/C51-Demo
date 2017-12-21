package com.uaes.esw.alarmprocess;

import com.uaes.esw.redisops.RedisOpsV2;
import com.uaes.esw.sparkjava.websocket.WebSocketServer;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AlarmProcess {

    private static String hash111111Name = "c51:111111:hash";
    private static String zset111111Name = "c51:111111:zset";
    private static Jedis jedisClient = RedisOpsV2.getOneJedisFromPool();

    private static int sendOutInterval = 3;

    private static String createRandomID(String value){
        return DigestUtils.md5Hex(value);
    }

    private static Map<String,String> getC51SnapshotFromRedis(String vinCode){
        return jedisClient.hgetAll(vinCode);
    }

    private static JSONObject createAlarmTemplate(){
        JSONObject alarmTemplate = new JSONObject();
        String localDateTimeStr = LocalDateTime.now().toString();
        alarmTemplate.put("time",localDateTimeStr);
        alarmTemplate.put("id",createRandomID(localDateTimeStr));
        JSONArray recommandArray = new JSONArray();
        alarmTemplate.put("recommand",recommandArray);
        JSONArray faultArray = new JSONArray();
        alarmTemplate.put("fault",faultArray);
        JSONArray warningArray = new JSONArray();
        alarmTemplate.put("warning",warningArray);
        JSONArray infoArray = new JSONArray();
        alarmTemplate.put("info",infoArray);
        return alarmTemplate;
    }

    private static JSONObject createEngineOilJSONObject(Map<String,String> c51Snapshot){
        JSONObject eoObj = new JSONObject();
        eoObj.put("part","engineoil");
        String engineOilRemainMileString = c51Snapshot.getOrDefault("eorm","2500");
        eoObj.put("engineOilRemainMile",engineOilRemainMileString);
        int engineOilLifeRate = Integer.valueOf(c51Snapshot.getOrDefault("reol","50"));
        double engineOilLifeDouble = 0.01*(double)engineOilLifeRate;
        eoObj.put("engineOilHealthRete",String.valueOf(engineOilLifeDouble));
        return eoObj;
    }




    public static boolean JadgeIncreseArrayWithRecursion(int[] array){
        return JadgeIncreseArrayWithRecursion(array, 0);
    }

    private static boolean JadgeIncreseArrayWithRecursion(int[] array, int begin){
        if (begin == array.length - 1){
            return true;
        }else{
            return array[begin] < array[begin + 1] &&  JadgeIncreseArrayWithRecursion(array, begin + 1);
        }
    }

    public static boolean JadgeArrayWithSame(int[] array){
        return JadgeArrayWithSame(array, 0);
    }

    private static boolean JadgeArrayWithSame(int[] array, int begin){
        if (begin == array.length - 1){
            return true;
        }else{
            return array[begin] == array[begin + 1] &&  JadgeArrayWithSame(array, begin + 1);
        }
    }


    private static boolean JadgeArrayWithLastOneBiggerThanFirstOne(int[] array){
        if(array[array.length-1] > array[0])
            return true;
        return false;
    }

    private static boolean JadgeArrayWithAllZero(int[] array){
        for(int i = 0;i<array.length;i++)
            if(array[i] != 0)
                return false;
        return true;
    }


    private static boolean JadgeArrayWithAllZeroLastOneNotZero(int[] array){
        int[] tempArray = new int[array.length-1];
        for(int i =0;i<array.length-1;i++)
            tempArray[i] = array[i];
        if(JadgeArrayWithAllZero(tempArray) && array[array.length-1] != 0)
            return true;
        return false;
    }

    private static boolean JadgeArrayWithAllZeroLastOneBiggerThan5(int[] array){
        int[] tempArray = new int[array.length-1];
        for(int i =0;i<array.length-1;i++)
            tempArray[i] = array[i];
        if(JadgeArrayWithAllZero(tempArray) && array[array.length-1] > 5)
            return true;
        return false;
    }

    //judge fuel filling
    private static long fuelFillBeginTime = 0;
    private static int fuelFillBeginVolume = 0;
    private static int arrayIndex = 0;
    public static void detectFuelFilling(){
        while(true){

            Set<String> last10msg = jedisClient.zrange(zset111111Name,-10,-1);
            int[] speedArray = new int[10];
            int[] gasBoxRemainArray = new int[10];
            long[] timestampArray = new long[10];
            last10msg.forEach(msg -> {
                speedArray[arrayIndex] = Integer.valueOf(new JSONObject(msg).getString("spd"));
                gasBoxRemainArray[arrayIndex] = Integer.valueOf(new JSONObject(msg).getString("gbr"));
                timestampArray[arrayIndex] = Long.valueOf(new JSONObject(msg).getString("uts"));
                arrayIndex++;
            });
            arrayIndex = 0;
            if(JadgeArrayWithAllZero(speedArray) && JadgeArrayWithLastOneBiggerThanFirstOne(gasBoxRemainArray)){
                fuelFillBeginTime = timestampArray[0];
                fuelFillBeginVolume = gasBoxRemainArray[0];
                System.out.println("Detect the fuel fill begin with "+fuelFillBeginVolume+" at "+fuelFillBeginTime);
                detectFuelFillingEnd();
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static long fuleFillEndTime = 0;
    private static int fuelFillEndVolume = 0;
    private static int arrayffIndex = 0;
    public static void detectFuelFillingEnd(){
        while(true){
            long nowTime = LocalDateTime.now().atZone(ZoneId.of("Asia/Shanghai"))
                    .toInstant().toEpochMilli();
            Set<String> fuelfillmsg = jedisClient.zrangeByScore(zset111111Name,fuelFillBeginTime,nowTime);
            int setffLength = fuelfillmsg.size();
            int[] speedffArray = new int[setffLength];
            int[] gasBoxRemainffArray = new int[setffLength];
            long[] timestampffArray = new long[setffLength];
            fuelfillmsg.forEach(msg -> {
                speedffArray[arrayffIndex] = Integer.valueOf(new JSONObject(msg).getString("spd"));
                gasBoxRemainffArray[arrayffIndex] = Integer.valueOf(new JSONObject(msg).getString("gbr"));
                timestampffArray[arrayffIndex] = Long.valueOf(new JSONObject(msg).getString("uts"));
                arrayffIndex++;
            });
            arrayffIndex = 0;
            if(JadgeArrayWithAllZeroLastOneNotZero(speedffArray)){
                fuleFillEndTime = timestampffArray[setffLength-1];
                fuelFillEndVolume = gasBoxRemainffArray[setffLength-1];
                System.out.println("Detect the fuel fill end with "+fuelFillEndVolume+" at "+fuleFillEndTime);
                JSONObject fuelfilled = new JSONObject();
                fuelfilled.put("part", "fuel");
                fuelfilled.put("status", "filled");
                fuelfilled.put("fuelfilled", fuelFillEndVolume-fuelFillBeginVolume);
                break;
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private static JSONObject analysisAlarmFromSnapshot(){

        JSONObject alarmObj = createAlarmTemplate();
        Map<String, String> c51Snapshot = getC51SnapshotFromRedis(hash111111Name);
        //battery
        int batteryCondition = Integer.valueOf(c51Snapshot.getOrDefault("bc","1"));
        if(batteryCondition == 0){
            JSONObject batteryWarning = new JSONObject();
            batteryWarning.put("part", "battery");
            batteryWarning.put("status", "low");
            alarmObj.getJSONArray("warning").put(batteryWarning);
        }
        //engineoil
        int changeOilSoon = Integer.valueOf(c51Snapshot.getOrDefault("ceos","0"));
        int changeOilImmediately = Integer.valueOf(c51Snapshot.getOrDefault("ceoi","0"));
        if(changeOilImmediately == 1){
            JSONObject engineOilObj = createEngineOilJSONObject(c51Snapshot);
            engineOilObj.put("status", "imme");
            alarmObj.getJSONArray("fault").put(engineOilObj);
        }
        else if (changeOilSoon == 1){
            JSONObject engineOilObj = createEngineOilJSONObject(c51Snapshot);
            engineOilObj.put("status", "asap");
            alarmObj.getJSONArray("warning").put(engineOilObj);
        }
        //sparking
        int sparkingPlug1DistressCondition = Integer.
                valueOf(c51Snapshot.getOrDefault("spdc1","0"));
        int sparkingPlug2DistressCondition = Integer.
                valueOf(c51Snapshot.getOrDefault("spdc2","0"));
        int sparkingPlug3DistressCondition = Integer.
                valueOf(c51Snapshot.getOrDefault("spdc3","0"));
        int sparkingPlug4DistressCondition = Integer.
                valueOf(c51Snapshot.getOrDefault("spdc4","0"));
        if(sparkingPlug1DistressCondition == 1 || sparkingPlug2DistressCondition == 1 ||
                sparkingPlug3DistressCondition == 1 || sparkingPlug4DistressCondition == 1)
        {
            JSONObject sparkingStatusObj = new JSONObject();
            JSONArray sparkingFaultArr = new JSONArray();
            sparkingStatusObj.put("detail",sparkingFaultArr);
            sparkingStatusObj.put("part","sparking");
            sparkingStatusObj.put("status","fault");
            if(sparkingPlug1DistressCondition == 1)
                sparkingFaultArr.put("first");
            if(sparkingPlug2DistressCondition == 1)
                sparkingFaultArr.put("second");
            if(sparkingPlug3DistressCondition == 1)
                sparkingFaultArr.put("third");
            if(sparkingPlug4DistressCondition == 1)
                sparkingFaultArr.put("forth");
            alarmObj.getJSONArray("fault").put(sparkingStatusObj);
        }
        //fuel-low
        int gasBoxMin = Integer.valueOf(c51Snapshot.getOrDefault("gbmn","7"));
        int gasBoxRemain = Integer.valueOf(c51Snapshot.getOrDefault("gbr","14"));
        if(gasBoxRemain < gasBoxMin){
            JSONObject fuelWarning = new JSONObject();
            fuelWarning.put("part", "fuel");
            fuelWarning.put("status", "low");
            alarmObj.getJSONArray("warning").put(fuelWarning);

        }
        return alarmObj;
    }

    private static void sendOutAlarm(){
        while(true){
            JSONObject alarmObj = analysisAlarmFromSnapshot();
            System.out.println(alarmObj);
            WebSocketServer.sendStrMessage(alarmObj.toString());

            try {
                TimeUnit.SECONDS.sleep(sendOutInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        WebSocketServer.start("/alarm",6767);
        sendOutAlarm();
    }
}
