package org.opennms.plugins.elasticsearch.rest.archive.manual;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.opennms.netmgt.xml.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenNMSRestEventsClientTest {
	private static final Logger LOG = LoggerFactory.getLogger(OpenNMSRestEventsClientTest.class);
	
	private String opennmsUrl="http://localhost:8980";

	private String userName="admin";

	private String passWord="admin";

	/**
	 * test if events can be received from OpenNMS ReST UI
	 */
	@Test
	public void test() {
		OnmsRestEventsClient onmsRestEventsClient = new OnmsRestEventsClient();
		
		onmsRestEventsClient.setOpennmsUrl(opennmsUrl);
		onmsRestEventsClient.setPassWord(passWord);
		onmsRestEventsClient.setUserName(userName);
		
		
		List<Event> receivedEvents = onmsRestEventsClient.getEvents(10, 1);
		
		LOG.debug("\nRECEIVED EVENTS ----------------------------------------");
		for(Event event : receivedEvents){
			
			LOG.debug(event.toString());

		}
		
	}

}
