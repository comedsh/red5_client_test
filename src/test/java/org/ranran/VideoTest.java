package org.ranran;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.red5.cache.impl.NoCacheImpl;
import org.red5.io.ITag;
import org.red5.io.ITagReader;
import org.red5.io.flv.IFLV;
import org.red5.io.flv.impl.FLVReader;
import org.red5.io.mp4.impl.MP4Reader;
import org.red5.server.messaging.IMessage;
import org.red5.server.net.rtmp.event.AudioData;
import org.red5.server.net.rtmp.event.FlexStreamSend;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.Invoke;
import org.red5.server.net.rtmp.event.Notify;
import org.red5.server.net.rtmp.event.Unknown;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.net.rtmp.message.Constants;
import org.red5.server.service.flv.impl.FLVService;
import org.red5.server.stream.FileStreamSource;
import org.red5.server.stream.message.RTMPMessage;

public class VideoTest {
	
	/**
	 * 
	 * 补充：RED5 处理 MP4 的三大组件，@see MP4, MP4Service, MP4Reader
	 * 
	 * 
	 * @throws IOException
	 */
	@Test
	public void testConvertMp4ToIMessage() throws IOException{
		
		File file = new File( VideoTest.class.getResource("monkey.mp4").getFile() );
		
		MP4Reader reader = new MP4Reader( file );
		
		while( reader.hasMoreTags() ){
			
			ITag tag = reader.readTag();
			
			IRTMPEvent msg = new VideoData( tag.getBody() );
			
			IMessage message = RTMPMessage.build( msg );
			
			Assert.assertTrue( message != null );
			
			System.out.println( tag.getBody() );
			
		}
		
	}
	
	@Test
	public void testFlv() throws IOException{
		
		File file = new File( VideoTest.class.getResource("testvid.flv").getFile() );
		
		FLVReader reader = new FLVReader( file );
		
		while( reader.hasMoreTags() ){
			
		}
		
	}
	
	@Test
	public void testFlv2() throws Exception{
		
		File file = new File( VideoTest.class.getResource("h264_mp3.flv").getFile() );
		
        FLVService service = new FLVService(); 
        
        service.setGenerateMetadata(true);
        
        IFLV flv = (IFLV) service.getStreamableFile( file );
        
        flv.setCache(NoCacheImpl.getInstance()); 
        
        ITagReader reader = flv.getReader();
        
        FileStreamSource stream = new FileStreamSource( reader );
        
        while (reader.hasMoreTags()) {
        	
            IRTMPEvent msg = null;
            
            ITag tag = reader.readTag();
            
            if (tag != null) {
            	
                int timestamp = tag.getTimestamp();
                
                switch (tag.getDataType()) {
                
                    case Constants.TYPE_AUDIO_DATA:
                    	
                        msg = new AudioData(tag.getBody());
                        
                        break;
                        
                    case Constants.TYPE_VIDEO_DATA:
                    	
                        msg = new VideoData(tag.getBody());
                        
                        break;
                        
                    case Constants.TYPE_INVOKE:
                    	
                        msg = new Invoke(tag.getBody());
                        
                        System.out.println( "~~~~~~~~~~~~~~ INVOKE command chunk message has found" );
                        
                        break;
                        
                    case Constants.TYPE_NOTIFY:
                    	
                        msg = new Notify(tag.getBody());
                        
                        System.out.println( "~~~~~~~~~~~~~~ NOTIFY command chunk message has found" );
                        
                        break;
                        
                    case Constants.TYPE_FLEX_STREAM_SEND:
                    	
                        msg = new FlexStreamSend(tag.getBody());
                        
                        System.out.println( "~~~~~~~~~~~~~~ FLEX STREAM chunk message has found" );
                        
                        break;
                        
                    default:
                    	
                        System.out.println( "Unexpected type? " + tag.getDataType() );
                        
                        msg = new Unknown(tag.getDataType(), tag.getBody());
                }
                
                msg.setTimestamp(timestamp);
                
                // RTMPMessage rtmpMsg = RTMPMessage.build(msg);
                
            } else {
                System.out.println("Tag was null");
            }        
		
        }
        
	}
	
}
