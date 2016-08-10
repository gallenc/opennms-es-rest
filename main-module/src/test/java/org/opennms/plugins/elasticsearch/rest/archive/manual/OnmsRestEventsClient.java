package org.opennms.plugins.elasticsearch.rest.archive.manual;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.opennms.netmgt.xml.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for importing events from OpenNMS ReST interface
 * (This is used to fill ES with old events from the OpenNMS database)
 * @author cgallen
 *
 */
public class OnmsRestEventsClient {

	private static final Logger LOG = LoggerFactory.getLogger(OnmsRestEventsClient.class);


	public static final String EVENTS_URI="/opennms/rest/events";

	private String opennmsUrl="http://localhost:8980";

	private String userName="admin";

	private String passWord="admin";

	public String getOpennmsUrl() {
		return opennmsUrl;
	}

	public void setOpennmsUrl(String opennmsUrl) {
		this.opennmsUrl = opennmsUrl;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassWord() {
		return passWord;
	}

	public void setPassWord(String passWord) {
		this.passWord = passWord;
	}


	public List<Event> getEvents(Integer limit, Integer offset){

		List<Event> retrievedEvents= new ArrayList<Event>();

		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, passWord));
		CloseableHttpClient httpclient = HttpClients.custom()
				.setDefaultCredentialsProvider(credsProvider)
				.build();

		String query = "";

		String limitStr= (limit==null) ? null : Integer.toString(limit);
		String offsetStr= (offset==null) ? null : Integer.toString(offset);

		if(limitStr!=null){
			query = "?limit="+limitStr;
			if(offset!=null){
				query=query+"&offset="+offsetStr;
			}
		} else {
			if(offset!=null) {
				query="?offset="+offsetStr;
			}
		}

		try {

			// importing events generated from opennms-webapp-rest/src/main/java/org/opennms/web/rest/v1/EventRestService.java

			HttpGet getRequest = new HttpGet(opennmsUrl+EVENTS_URI+query);
			getRequest.addHeader("accept", "application/XML");

			LOG.debug("Executing request " + getRequest.getRequestLine());

			CloseableHttpResponse response = httpclient.execute(getRequest);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatusLine().getStatusCode());
			}
			String responseStr=null;

			try {
				LOG.debug("----------------------------------------");
				LOG.debug(response.getStatusLine().toString());

				responseStr= EntityUtils.toString(response.getEntity());
				LOG.debug(responseStr);
				LOG.debug("----------------------------------------");
			} finally {
				response.close();
			}

			JAXBContext jc = JAXBContext.newInstance(XmlOnmsEventCollection.class);

			Unmarshaller unmarshaller = jc.createUnmarshaller();
			StringReader reader = new StringReader(responseStr);
			XmlOnmsEventCollection eventCollection = (XmlOnmsEventCollection) unmarshaller.unmarshal(reader);
			LOG.debug("received xml events ----------------------------------------");
			for(XmlOnmsEvent xmlOnmsevent : eventCollection){
				LOG.debug("event:"+xmlOnmsevent);
			}

			//			JAXBContext jc = JAXBContext.newInstance(Events.class);
			//
			//	        Unmarshaller unmarshaller = jc.createUnmarshaller();
			//	        StringReader reader = new StringReader(responseStr);
			//	        Events eventCollection = (Events) unmarshaller.unmarshal(reader);
			//			
			//			for(Event event : eventCollection.getEventCollection()){
			//				System.out.println("event:"+event);
			//			}

			LOG.debug("converting to events ----------------------------------------");
			for(XmlOnmsEvent xmlOnmsevent : eventCollection){
				Event event= xmlOnmsevent.toEvent();


				LOG.debug(event.toString());

				retrievedEvents.add(event);
			}

		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			try {
				httpclient.close();
			} catch (IOException e) { }
		}
		return retrievedEvents;
	}
}
