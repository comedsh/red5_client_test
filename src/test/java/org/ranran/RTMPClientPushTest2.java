package org.ranran;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.red5.cache.impl.NoCacheImpl;
import org.red5.client.net.rtmp.INetStreamEventHandler;
import org.red5.client.net.rtmp.RTMPClient;
import org.red5.io.ITagReader;
import org.red5.io.flv.IFLV;
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
	
	String host = "10.211.55.8";
	
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
		    	
				File file = new File( VideoTest.class.getResource("h264_mp3.flv").getFile() );
				
				logger.debug( "test: {}", file );
				
				try {
					
		            FLVService service = new FLVService(); 
		            
		            IFLV flv = (IFLV) service.getStreamableFile( file );
		            
		            flv.setCache(NoCacheImpl.getInstance()); 
		            
		            ITagReader reader = flv.getReader();
		            
		            FileStreamSource stream = new FileStreamSource( reader );
		            
		            while( stream.hasMore() ){
		            	
		            	IRTMPEvent event = stream.dequeue();
		            	
		            	RTMPMessage message = RTMPMessage.build( event );
		            	
		            	this.publishStreamData( streamId, message );		
		            	
		            	System.out.println( ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> published one record: " + event.toString() );
		            	
						if( !stream.hasMore() ){
							
							long start = System.currentTimeMillis();
							
							System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> read to the end, reconstruct the file ");
							
							// 重置 reader 即可
					        reader = flv.getReader();
					        
					        stream = new FileStreamSource( reader );
					        
					        long end = System.currentTimeMillis();
					        
					        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> read to the end, reconstruct the file completed, time spent: " + ( end - start ) +" ms ");
							
						}            	
		            	
		            }
		            
				} catch (Exception e) {
					
					e.printStackTrace();
					
				}			

		        
		    } else if (StatusCodes.NS_UNPUBLISHED_SUCCESS.equals(code)) {
		
		    }
		    
		}catch(Exception e){
			
			e.printStackTrace();
		
		}
	}  

    @Override 
	public void connectionOpened(RTMPConnection conn) { 
	  
    	super.connectionOpened(conn);
	  
	} 
    
	public static void main(String[] args) {
		
	    new RTMPClientPushTest2();
	    
	}  
	
}

