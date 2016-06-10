package org.ranran;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.red5.cache.impl.NoCacheImpl;
import org.red5.client.net.rtmp.INetStreamEventHandler;
import org.red5.client.net.rtmp.RTMPClient;
import org.red5.io.ITagReader;
import org.red5.io.flv.IFLV;
import org.red5.io.mp4.IMP4;
import org.red5.io.utils.ObjectMap;
import org.red5.server.api.event.IEvent;
import org.red5.server.api.event.IEventDispatcher;
import org.red5.server.api.service.IPendingServiceCall;
import org.red5.server.api.service.IPendingServiceCallback;
import org.red5.server.net.rtmp.RTMPConnection;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.Notify;
import org.red5.server.net.rtmp.status.StatusCodes;
import org.red5.server.service.flv.impl.FLVService;
import org.red5.server.service.mp4.impl.MP4Service;
import org.red5.server.stream.FileStreamSource;
import org.red5.server.stream.message.RTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Not use a new thread to collect the video frames.
 * 
 * @author 商洋
 *
 */
public class RTMPClientPushTest2 extends RTMPClient implements INetStreamEventHandler, IPendingServiceCallback, IEventDispatcher {  

	
	private static final Logger logger = LoggerFactory.getLogger(RTMPClientPushTest.class); 
	
	String host = "127.0.0.1";
	
	String app = "my-first-red5-example";
	
	String streamName = "mystream";
	
	String LIVE_MODE = "live";
	
	int port = 1935;  // the RTMP port
	
	// 服务器会为每一个 Stream (流媒体产生的流) 分配一个唯一的 ID
	Number streamId = 0;
	
	public static final int FRAME_MILLISECONDS_INTERVAL = 10; // 1 秒钟 100 帧的速度发送
	
	public RTMPClientPushTest2() {
		
	    super();
	    
	    Map<String, Object> map = makeDefaultConnectionParams( host, port, app );
	    
	    connect(host, 1935, map, this);
	    
	}  
	
	public void dispatchEvent(IEvent arg0) {
		
	}  
	
	@SuppressWarnings("unchecked")
	public void resultReceived( IPendingServiceCall call ) { 
		
	    Object result = call.getResult();
	    
	    // TODO add the bad name scenario
	    
	    if (result instanceof ObjectMap) {
	    	
	        if ("connect".equals(call.getServiceMethodName())) {  
	        	
	        	System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> connect success, send create steam command to server");
	        	
	            createStream(this);
	            
	        }  
	        
	    } else {
	    	
	        if ("createStream".equals( call.getServiceMethodName()) ) {
	        	
	        	System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> create stream success, send the publish command to server");
	        	
	        	// 如果有返回值，返回值一定是一个 @See Number 类型
	            if ( result instanceof Number ) {
	            	
	            	/*
	            	 * 获取服务器端分配的 stream id @TODO 了解 stream id 的创建过程
	            	 * 发送 publish 指令给服务器端，告诉服务器，我准备要 publish 了
	            	 */
	                streamId = (Number) result;
	                
	                publish( streamId, streamName, LIVE_MODE, this );
	                
//	                invoke( "getRoomsInfo", this );
	                
	            } else {
	            	
	                disconnect();
	                
	            }  
	            
	        } else if ("getRoomsInfo".equals(call.getServiceMethodName())) {
	        	
	            ArrayList<String> list = (ArrayList<String>) result;
	            
	            for (int i = 0; i < list.size(); i++) {
	            	
	                System.out.println(list.get(i));
	                
	            }  
	            
	        }  
	    }  
	}  
	
	public void onStreamEvent( Notify notify ) {   
		
		try{
			
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ready, pulish the stream data from frameBuffer ");
			
		    ObjectMap<?, ?> map = (ObjectMap<?, ?>) notify.getCall().getArguments()[0];
		    
		    String code = (String) map.get("code");
		    
		    System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  the code is  " + code );
		    
		    if (StatusCodes.NS_PUBLISH_START.equals(code)) {
				
				try {
					
					processFLVSample();
					
					// processMP4Sample();
					
			            
				} catch (Exception e) {
					
					e.printStackTrace();
					
				}			

		        
		    } else if (StatusCodes.NS_UNPUBLISHED_SUCCESS.equals(code)) {
		
		    }
		    
		}catch(Exception e){
			
			e.printStackTrace();
		
		}
	}  

	void processFLVSample() throws Exception{
		
		File file = new File( VideoTest.class.getResource("h264_mp3.flv").getFile() );
		
		logger.debug( "test: {}", file );
		
        FLVService service = new FLVService(); 
        
        IFLV flv = (IFLV) service.getStreamableFile( file );
        
        flv.setCache(NoCacheImpl.getInstance()); 
        
        ITagReader reader = flv.getReader();
        
        FileStreamSource stream = new FileStreamSource( reader );
        
        // new logic, 根据相邻两帧之间的 gap timestamp 来决定休眠多久后发送...
        // -> 看来 IRTMPEvent.timestamp 对于播放的质量很重要，决定了播放是否卡顿...
        // 难道 Flash Player 一旦得到数据马上就播放？都不判断下，前后两帧数据的间隔时间？缓存好了以后，在播放？是不是直播流做不到？
        // 疑问，这种方式是解决了播放卡顿的问题，但是，问题是，怎么控制流畅度，却取决我我发送的间隔了，而不是在 Header 中有 timestamp 这个属性由来控制播放的速度？
        
        int previousTs = 0;
        
        int gapTs = 0;
        
        while( stream.hasMore() ){
        	
        	IRTMPEvent event = stream.dequeue();
        	
        	RTMPMessage message = RTMPMessage.build( event );
        	
        	if( previousTs == 0 ){
        		
        		this.publishStreamData( streamId, message );
        		
        	}else{

            	gapTs = event.getTimestamp() - previousTs;
            	
            	if( gapTs - 3 > 0 )TimeUnit.MILLISECONDS.sleep( gapTs - 3 ); // 假设每次发送浪费三毫秒的时间
            	
            	this.publishStreamData(streamId, message);
            	
        	}
        	
        	previousTs = event.getTimestamp();
        	
//        	this.publishStreamData( streamId, message );
//        	
//        	TimeUnit.MILLISECONDS.sleep(24); // 每秒钟发送 30 帧数据
        	
        	System.out.println( ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> published one record: " + event.toString() + "; gapTs = " + gapTs + "; Header Timer:" + ( event.getHeader() != null ? event.getHeader().getTimer() : "" ) );
        	
			if( !stream.hasMore() ){
				
				previousTs = 0;
				
				gapTs = 0;
				
				long start = System.currentTimeMillis();
				
				System.err.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> read to the end, reconstruct the file ");
				
				// 重置 reader 即可
		        reader = flv.getReader();
		        
		        stream = new FileStreamSource( reader );
		        
		        long end = System.currentTimeMillis();
		        
		        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> read to the end, reconstruct the file completed, time spent: " + ( end - start ) +" ms ");
				
			}            	
        	
        }
		
	}
	
	void processMP4Sample() throws Exception{
		
		File file = new File( VideoTest.class.getResource("monkey.flv").getFile() );
		
		MP4Service service = new MP4Service(); 
        
        IMP4 mp4 = (IMP4) service.getStreamableFile( file );
        
        // no cacheable
        // mp4.setCache(NoCacheImpl.getInstance()); 
        
        ITagReader reader = mp4.getReader();
        
        FileStreamSource stream = new FileStreamSource( reader );
        
        while( stream.hasMore() ){
        	
        	IRTMPEvent event = stream.dequeue();
        	
        	RTMPMessage message = RTMPMessage.build(event);
        	
        	this.publishStreamData( streamId, message );
        	
			if( !stream.hasMore() ){
				
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> read to the end, reconstruct the file and re-read it ");
				
				// 重置 reader 即可, MP4 重复播放一次之间的间隔要长些。等等.. 
		        reader = mp4.getReader();
		        
		        stream = new FileStreamSource( reader );
				
			}            	
        	
        }
        
		RTMPClientPushTest.frameCollectedCompleted = true;
		
	}
	
	
    @Override 
	public void connectionOpened(RTMPConnection conn) { 
	  
    	super.connectionOpened(conn);
	  
	} 
    
	public static void main(String[] args) {
		
	    new RTMPClientPushTest2();
	    
	}  
	
}

