package org.ranran;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.red5.io.ITag;
import org.red5.io.flv.impl.FLVReader;
import org.red5.io.mp4.impl.MP4Reader;
import org.red5.server.messaging.IMessage;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.VideoData;
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
	
}
