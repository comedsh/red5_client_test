package org.ranran;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.red5.cache.impl.NoCacheImpl;
import org.red5.client.net.rtmp.INetStreamEventHandler;
import org.red5.client.net.rtmp.RTMPClient;
import org.red5.io.ITag;
import org.red5.io.ITagReader;
import org.red5.io.flv.IFLV;
import org.red5.io.mp4.IMP4;
import org.red5.io.mp4.impl.MP4Reader;
import org.red5.io.utils.ObjectMap;
import org.red5.server.api.event.IEvent;
import org.red5.server.api.event.IEventDispatcher;
import org.red5.server.api.service.IPendingServiceCall;
import org.red5.server.api.service.IPendingServiceCallback;
import org.red5.server.messaging.IMessage;
import org.red5.server.net.rtmp.RTMPConnection;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.Notify;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.net.rtmp.status.StatusCodes;
import org.red5.server.service.flv.impl.FLVService;
import org.red5.server.service.mp4.impl.MP4Service;
import org.red5.server.stream.FileStreamSource;
import org.red5.server.stream.message.RTMPMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * 为什么用 frameBuffer，另起一个线程来读取视频流数据，并缓存到 frameBuffer 中？
 * 
 * 	1. 常规的思维逻辑是，从视频流中采集一帧数据，然后发送一帧数据给服务器 ( RMTP 是基于 TCP 协议的 )
 *     问题是，如果服务器端口延迟或者堵塞，客户端发送给服务器迟迟没能得到响应，就一直被阻塞，而视频流却源源不断的在涌入（设想，视频流从摄像头来），那么一直被阻塞在这里，那么新的视频数据流迟迟不能发送，如果缓存未发送的数据，
 *     极为可能将客户端应用程序的内存给撑爆... 由此想到一种新的做法 #2 ，开启一个线程来专门处理incoming的流数据，这样，就不会造成堵塞。
 *     
 *  2. 开启一个新的线程来专门处理流数据，并且将数据缓存在 frameBuffer 中
 *     同样，需要考虑到，如果发送数据主线程被堵塞，大量数据被缓存到 frameBuffer 而造成内存溢出，怎么解决？
 *     解决的办法是，设置一个阈值，如果到达某个阈值，就不再缓存新的数据知道主线程将数据从 frameBuffer poll 出来，并发送给服务器后 有更多的空间够 frameBuffer 缓存为止。
 *    
 * 
 * 
 * 
 * @author shangyang
 *
 */
public class RTMPClientTest extends RTMPClient implements INetStreamEventHandler, IPendingServiceCallback, IEventDispatcher {  

	/*
	 * @TODO frameBuffer 的数据怎么来？
	 */
	private ConcurrentLinkedQueue<IMessage> frameBuffer = new ConcurrentLinkedQueue<IMessage>();
	
	public static volatile int FRAME_BUFFER_THREDHOLE = 100; // frame buffer 的缓存上限  
	
	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(RTMPClientTest.class); 
	
	String host = "127.0.0.1";
	
	String app = "my-first-red5-example";
	
	String streamName = "mystream";
	
	String LIVE_MODE = "live";
	
	int port = 1935;  // the RTMP port
	
	public static volatile boolean frameCollectedCompleted = false;
	
	// 服务器会为每一个 Stream (流媒体产生的流) 分配一个唯一的 ID
	Number streamId = 0;
	
	public static final int FRAME_MILLISECONDS_INTERVAL = 10; // 1 秒钟 100 帧的速度发送
	
	public RTMPClientTest() {
		
	    super();
	    
	    Map<String, Object> map = makeDefaultConnectionParams( host, port, app );
	    
	    connect(host, 1935, map, this);
	    
	}  
	
	public void dispatchEvent(IEvent arg0) {
		
	}  
	
	@SuppressWarnings("unchecked")
	public void resultReceived( IPendingServiceCall call ) { 
		
	    Object result = call.getResult();
	    
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
		    	
		    	FrameBufferGenerator generator = new FrameBufferGenerator(frameBuffer);
		    	
		    	generator.start(); // 开启一个线程，循环往复的读取 一个 mp4 文件的帧
		    	
		        IMessage message = null;
		        
		        /**
		         * To understand,
		         * 
		         * java.lang.OutOfMemoryError: Java heap space
				        at java.nio.HeapByteBuffer.<init>(HeapByteBuffer.java:57)
				        
				 * 当在变采集边发送的时候，抛出这个错误；发现问题的根本原因可能是，采样太快，而发送过慢，导致大量的数据堆积在内存，而没有发出去并清空；
				 * 
				 * 所以，准备采集得慢一点；先测试用 10ms 采集一次 - 既是设置采样的频率；最好的办法是，检测两次 publish stream data 的时间差，然后考虑采样的频率。     
				 * 
				 * 设置以后，采集和发送正常了，基本上是一次采集，然后一次发送。
		         * 
		         */
		        
		        /**
		         * 注意，这里是并发编程，下面的代码考虑到两层逻辑，如下
		         * 
		         * 1. 第一个判断条件，表示视频数据正在另一个线程中采集；这个条件保证，边采集，边发送
		         * 2. 第二个判断条件，有一种可能性 当 frameCollectedCompleted = true，视频数据采集完成了；但是这个时候 frameBuffer 里面可能仍有数据，所以需要判断 frameBuffer 是否仍有待发送的数据。 
		         */
		        while ( frameCollectedCompleted == false  ) {
		        	
		        	while(( message = frameBuffer.poll() ) != null ){
		        		
		        		System.out.println(" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> push success；frameCollectedCompleted == false ~~ ");
		        		
		        		/*
			        	 * 真正的开始 publish 数据了
			        	 */
		        		
		            	this.publishStreamData( streamId, message );
		            	
		        	}

		        }
		        
		        if( frameCollectedCompleted == true ){
		        	
		        	while(( message = frameBuffer.poll() ) != null ){
		        		
		        		System.out.println(" >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> push success；frameCollectedCompleted == true ~~ ");
		        		
		        		/*
			        	 * 真正的开始 publish 数据了
			        	 */
		        		
		            	this.publishStreamData( streamId, message );
		            	
		        	}	        	
		        	
		        }
		        
		        disconnect(); // if completed, then disconnect.
		        
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
		
	    new RTMPClientTest();
	    
	}  
	
}

/**
 * 
 * 像一个永动机一样，不断循环的制造 FrameBuffers.
 * 
 * @author shangyang
 *
 */
class FrameBufferGenerator extends Thread{

	ConcurrentLinkedQueue<IMessage> frameBuffer;
	
	private static final Logger logger = LoggerFactory.getLogger(RTMPClientTest.class); 
	
	public FrameBufferGenerator( ConcurrentLinkedQueue<IMessage> frameBuffer ){
		
		this.frameBuffer = frameBuffer;
		
	}
	
	@Override
	public void run() {
		
		// collectMP4();
		
		collectFlv();

	}
	
	/**
	 * Copy the logic from collectFLV()
	 */
	void collectMP4(){
		
		File file = new File( VideoTest.class.getResource("monkey.mp4").getFile() );
		
		logger.debug( "test: {}", file );
		
		try {
			
			MP4Service service = new MP4Service(); 
            
            IMP4 mp4 = (IMP4) service.getStreamableFile( file );
            
            // no cacheable
            // mp4.setCache(NoCacheImpl.getInstance()); 
            
            ITagReader reader = mp4.getReader();
            
            FileStreamSource stream = new FileStreamSource( reader );
            
            while( stream.hasMore() ){
            	
            	IRTMPEvent event = stream.dequeue();
            	
            	RTMPMessage message = RTMPMessage.build(event);
            	
            	// 判断是否已经超过了缓存的上限，这里我选择的做法是丢弃，更符合摄像直播的场景.. 
            	if( frameBuffer.size() < RTMPClientTest.FRAME_BUFFER_THREDHOLE ){
            	
	            	frameBuffer.add( message );
	            	
	            	System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> record one frame into frame buffer");
            	
            	}
            	
            	TimeUnit.MILLISECONDS.sleep( RTMPClientTest.FRAME_MILLISECONDS_INTERVAL );
            	
            	// 循环读取文件内容，模拟视频流读取，便于调试直播
            	// 需要注意的是，如果是 record，记录在服务器上的视频文件并不会累加，只会记录一次播放完整的记录。我猜想，服务器比较智能，在存储一个新文件的时候，比对了流媒体的指纹，所以不让重复保存
            	// 但是，直播不影响，可以循环的直播。-> Yes，我的目的达到了。
				if( !stream.hasMore() ){
					
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> read to the end, reconstruct the file and re-read it ");
					
					// 重置 reader 即可, MP4 重复播放一次之间的间隔要长些。等等.. 
			        reader = mp4.getReader();
			        
			        stream = new FileStreamSource( reader );
					
				}            	
            	
            }
            
			RTMPClientTest.frameCollectedCompleted = true;
			
		} catch (Exception e) {
			
			e.printStackTrace();
			
		}			
	}
		
	
	/**
	 * Golden code. 完整的将 flv 视频文件上传到了服务器，并且可以正常的回放
	 * 
	 * 注意，FLVService 的用法，这个太重要了，否则读取的视频，播放有问题，而且没办法直播.. 之前的代码参看 @see {@link FrameBufferGenerator#collectMp4Reference()}
	 * 
	 */
	void collectFlv(){
		
		File file = new File( VideoTest.class.getResource("testvid.flv").getFile() );
		
		logger.debug( "test: {}", file );
		
		try {
			
            FLVService service = new FLVService(); 
            
            IFLV flv = (IFLV) service.getStreamableFile( file );
            
            flv.setCache(NoCacheImpl.getInstance()); 
            
            ITagReader reader = flv.getReader();
            
            FileStreamSource stream = new FileStreamSource( reader );
            
            while( stream.hasMore() ){
            	
            	// 下面一行，是为了进行调试所用，真正环境当中，记得要删除 
            	
            	if( frameBuffer.size() >= 1 ) continue; // TRY III, 确保在没有发送成功之前，frame buffer 里面有且仅有一帧的数据可以读取到； 便于 debug.
            	
            	IRTMPEvent event = stream.dequeue();
            	
            	RTMPMessage message = RTMPMessage.build(event);
            	
            	// 判断是否已经超过了缓存的上限，这里我选择的做法是丢弃，更符合摄像直播的场景.. 
            	if( frameBuffer.size() < RTMPClientTest.FRAME_BUFFER_THREDHOLE ){
            	
	            	frameBuffer.add( message ); // TRY II: 设置 Debug Point，控制数据一帧一帧的进行发送，便于 Debug. 不行，如果 debug 到这里，就不能 debug 主进程了..
	            	
	            	System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> record one frame into frame buffer");
            	
            	} 
            	
            	
            	// Thread.currentThread().wait(); // TRY I: 发送一帧数据过去，便于来进行 debug；不行，如果我想要看下一帧的时候，看不到，没人能够 invoke 它
            	
            	// TimeUnit.MILLISECONDS.sleep( RTMPClientTest.FRAME_MILLISECONDS_INTERVAL );
            	
            	// 循环读取文件内容，模拟视频流读取，便于调试直播
            	// 需要注意的是，如果是 record，记录在服务器上的视频文件并不会累加，只会记录一次播放完整的记录。我猜想，服务器比较智能，在存储一个新文件的时候，比对了流媒体的指纹，所以不让重复保存
            	// 但是，直播不影响，可以循环的直播。-> Yes，我的目的达到了。
				if( !stream.hasMore() ){
					
					System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> read to the end, reconstruct the file and re-read it ");
					
					// 重置 reader 即可
			        reader = flv.getReader();
			        
			        stream = new FileStreamSource( reader );
					
				}            	
            	
            }
            
			RTMPClientTest.frameCollectedCompleted = true;
			
		} catch (Exception e) {
			
			e.printStackTrace();
			
		}			
	}
	
	/**
	 * 
	 * 自己构造 Reader，然后从 reader 直接获取视频的原始数据，并发送给 RED5.. 结果问题是，视频播放不流畅，非常快，感觉像是没有视频控制.. 
	 * 
	 * 注意：必须用 @see IStreamableFileService 来构建流文件的读取操作，既 @see FLVService, MP4Service
	 * 
	 * @see FrameBufferGenerator#collectFlv() 正确的做法
	 * 
	 */
	void collectMp4Reference(){
		
		File file = new File( VideoTest.class.getResource("monkey.mp4").getFile() );
		
		try {
			
			MP4Reader reader = new MP4Reader( file );
		
			while( reader.hasMoreTags() ){
				
				ITag tag = reader.readTag();
				
				IRTMPEvent msg = new VideoData( tag.getBody() );
				
				IMessage message = RTMPMessage.build( msg );
				
				frameBuffer.add( message );
				
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> record one frame into frame buffer");
				
				// Thread.currentThread().suspend(); // 放一帧数据，用来调试
				
				TimeUnit.MILLISECONDS.sleep( RTMPClientTest.FRAME_MILLISECONDS_INTERVAL );
				
				// 之前犯过的错误 TimeUnit.SECONDS.sleep(20); 是休眠 20 ms
				
				// 循环读取文件内容
				if( !reader.hasMoreTags() ){
					
					System.out.println(" read to the end, reconstruct the file and re-read it ");
					
					reader = new MP4Reader( file );
					
				}
					
			}		
			
			RTMPClientTest.frameCollectedCompleted = true;
			
		} catch (Exception e) {
			
			e.printStackTrace();
			
		}	
		
	}	
}

