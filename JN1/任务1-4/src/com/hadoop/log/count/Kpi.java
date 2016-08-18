package com.hadoop.log.count;
//172.22.49.44 [08/Sep/2015:00:19:16 +0800] "GET /tour/category/query HTTP/1.1" GET 200 124 8
public class Kpi {
    private String remote_addr;// 璁板綍瀹㈡埛绔殑ip鍦板潃
    private String time_local;// 璁板綍璁块棶鏃堕棿涓庢椂鍖�
    private String request;// 璁板綍璇锋眰鐨剈rl涓巋ttp鍗忚
    private String status;// 璁板綍璇锋眰鐘舵�侊紱鎴愬姛鏄�200
    private String body_bytes_sent;// 璁板綍鍙戦�佺粰瀹㈡埛绔枃浠朵富浣撳唴瀹瑰ぇ灏�
    private String method;//璇锋眰鏂规硶 get post
    private String response_time;//xiang ying shi jian 
     
    public String getMethod() {
        return method;
    }
    public void setMethod(String method) {
        this.method = method;
    }
    public String getRemote_addr() {
        return remote_addr;
    }
    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }
    public String getTime_local() {
        return time_local;
    }
    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }
    public String getRequest() {
        return request;
    }
    public void setRequest(String request) {
        this.request = request;
    }
    public String getStatus() {
        return status;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }
    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }
    public String getHResponse_time() {
        return response_time;
    }
    public void setResponse_time(String response_time) {
        this.response_time = response_time;
    }
    @Override
    public String toString() {
        return "Kpi [remote_addr=" + remote_addr + ", time_local=" + time_local + ", request="
                + request + ", status=" + status + ", body_bytes_sent="
                + body_bytes_sent + ", method=" + method
                + ", response_time=" + response_time + "]";
    }     
}