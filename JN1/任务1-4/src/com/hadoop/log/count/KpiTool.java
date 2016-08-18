package com.hadoop.log.count;
public class KpiTool {
    /***
     * line璁板綍杞寲鎴恔pi瀵硅薄
     * @param line 鏃ュ織鐨勪竴鏉¤褰�
     * */
    public static Kpi Line2Kpi(String line){
        String[] elementList = line.split(" ");
        Kpi kpi = new Kpi();
        if(line.indexOf("HTTP") == -1){
        	kpi.setRemote_addr(elementList[0]);
        	kpi.setTime_local(elementList[1].substring(1) + " " + elementList[2].substring(0,5));
        	kpi.setMethod(elementList[5]);
        	kpi.setRequest(elementList[4]);
        	kpi.setStatus(elementList[6]);
        	kpi.setBody_bytes_sent(elementList[7]);
        	kpi.setResponse_time(elementList[8]);
        }
        else{
	        kpi.setRemote_addr(elementList[0]);
	        kpi.setTime_local(elementList[1].substring(1) +  " " + elementList[2].substring(0,5));
	        kpi.setMethod(elementList[6]);
	        kpi.setRequest(elementList[4]);
	        kpi.setStatus(elementList[7]);
	        kpi.setBody_bytes_sent(elementList[8]);
	        kpi.setResponse_time(elementList[9]);
        }
        return kpi;
    }
}
