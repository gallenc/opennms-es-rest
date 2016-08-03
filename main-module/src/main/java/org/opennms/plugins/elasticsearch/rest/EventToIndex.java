package org.opennms.plugins.elasticsearch.rest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opennms.netmgt.events.api.EventParameterUtils;

import javax.xml.bind.DatatypeConverter;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opennms.plugins.elasticsearch.rest.NodeCache;
import org.opennms.netmgt.model.OnmsSeverity;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Parm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventToIndex {


	private static final Logger LOG = LoggerFactory.getLogger(EventToIndex.class);

	public static final String ALARM_INDEX_NAME = "opennms-alarms";
	public static final String ALARM_EVENT_INDEX_NAME = "opennms-alarm-events";
	public static final String EVENT_INDEX_NAME = "opennms-events";
	public static final String ALARM_INDEX_TYPE = "alarmdata";
	public static final String EVENT_INDEX_TYPE = "eventdata";

	// stem of all alarm change notification uei's
	public static final String ALARM_NOTIFICATION_UEI_STEM = "uei.opennms.org/plugin/AlarmChangeNotificationEvent";

	// uei definitions of alarm change events
	public static final String ALARM_DELETED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/AlarmDeleted";
	public static final String ALARM_CREATED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/NewAlarmCreated";
	public static final String ALARM_SEVERITY_CHANGED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/AlarmSeverityChanged";
	public static final String ALARM_ACKNOWLEDGED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/AlarmAcknowledged";
	public static final String ALARM_UNACKNOWLEDGED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/AlarmUnAcknowledged";
	public static final String ALARM_SUPPRESSED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/AlarmSuppressed";
	public static final String ALARM_UNSUPPRESSED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/AlarmUnSuppressed";
	public static final String ALARM_TROUBLETICKET_STATE_CHANGE_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/TroubleTicketStateChange";
	public static final String ALARM_CHANGED_EVENT = "uei.opennms.org/plugin/AlarmChangeNotificationEvent/AlarmChanged";

	private static final String OLD_ALARM_VALUES="oldalarmvalues";
	private static final String NEW_ALARM_VALUES="oldalarmvalues";

	private boolean logEventDescription=false;
	private NodeCache nodeCache=null;
	private String elasticsearchCluster="opennms";

	private JestClient jestClient = null;

	private RestClientFactory restClientFactory = null;

	public boolean isLogEventDescription() {
		return logEventDescription;
	}

	public void setLogEventDescription(boolean logEventDescription) {
		this.logEventDescription = logEventDescription;
	}

	public NodeCache getNodeCache() {
		return nodeCache;
	}

	public void setNodeCache(NodeCache cache) {
		this.nodeCache = cache;
	}

	public RestClientFactory getRestClientFactory() {
		return restClientFactory;
	}

	public void setRestClientFactory(RestClientFactory restClientFactory) {
		this.restClientFactory = restClientFactory;
	}
	
	/**
	 * @return the elasticsearchCluster
	 */
	public String getElasticsearchCluster() {
		return elasticsearchCluster;
	}

	/**
	 * @param elasticsearchCluster the elasticsearchCluster to set
	 */
	public void setElasticsearchCluster(String elasticsearchCluster) {
		this.elasticsearchCluster = elasticsearchCluster;
	}

	/**
	 * returns a singleton jest client from factory for use by this class
	 * @return
	 */
	private JestClient getJestClient(){
		if (jestClient==null) {
			synchronized(this){
				if (jestClient==null){
					if (restClientFactory==null) throw new RuntimeException("JestClientFactory must be set");
					jestClient= restClientFactory.getJestClient();
				}
			}
		}
		return jestClient;
	}

	public void destroy(){
		if (jestClient!=null)
			try{
				jestClient.shutdownClient();
			}catch (Exception e){}
	}


	/** 
	 * this handles the incoming event and deals with it as an alarm change event or a normal event
	 * @param event
	 */
	public void forwardEvent(Event event){

		try{
			maybeRefreshCache(event);

			// handling uei definitions of alarm change events

			String uei=event.getUei();
			Index alarmIndex=null;
			Index eventIndex=null;
			DocumentResult alarmIndexresult=null;
			DocumentResult eventIndexresult=null;
			
			// if alarm change notification then handle change
			// change alarm index and add event to alarm change event index
			if(uei.startsWith(ALARM_NOTIFICATION_UEI_STEM)) {			

				if (ALARM_CREATED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Created Event to ES:"+event.toString());

				} else if( ALARM_DELETED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Deleted Event to ES:"+event.toString());

				} else if (ALARM_SEVERITY_CHANGED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Changed Severity Event to ES:"+event.toString());

				} else if (ALARM_ACKNOWLEDGED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Acknowledged Event to ES:"+event.toString());

				} else if (ALARM_UNACKNOWLEDGED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Unacknowledged Event to ES:"+event.toString());

				} else if (ALARM_SUPPRESSED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Suppressed Event to ES:"+event.toString());

				} else if (ALARM_UNSUPPRESSED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Unsuppressed Event to ES:"+event.toString());

				} else if (ALARM_TROUBLETICKET_STATE_CHANGE_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm TroubleTicked state changed Event to ES:"+event.toString());

				} else if (ALARM_CHANGED_EVENT.equals(uei)){
					if (LOG.isDebugEnabled()) LOG.debug("Sending Alarm Changed Event to ES:"+event.toString());

				}

				alarmIndex = populateAlarmIndexBodyFromAlarmChangeEvent(event, ALARM_INDEX_NAME, ALARM_INDEX_TYPE);
				alarmIndexresult = getJestClient().execute(alarmIndex);
				
				if(LOG.isDebugEnabled()) {
					if (alarmIndexresult==null) {
						LOG.debug("returned dresult==null");
					} else{
						LOG.debug("Alarm sent to es index:"+ALARM_INDEX_NAME+" type:"+ ALARM_INDEX_TYPE+" received search dresult: "+alarmIndexresult.getJsonString()
						+ "\n   response code:" +alarmIndexresult.getResponseCode() 
						+ "\n   error message: "+alarmIndexresult.getErrorMessage());
					}
				}

				eventIndex = populateEventIndexBodyFromEvent(event, ALARM_EVENT_INDEX_NAME, EVENT_INDEX_TYPE);
				eventIndexresult = getJestClient().execute(eventIndex);
				
				if(LOG.isDebugEnabled()) {
					if (alarmIndexresult==null) {
						LOG.debug("returned dresult==null");
					} else{
					LOG.debug("event sent to es index:"+ALARM_EVENT_INDEX_NAME+" type:"+ EVENT_INDEX_TYPE+" received search dresult: "+eventIndexresult.getJsonString()
						+ "\n   response code:" +eventIndexresult.getResponseCode() 
						+ "\n   error message: "+eventIndexresult.getErrorMessage());
					}
				}

				// else handle all other event types
			} else {

				// only send events to ES which are persisted to database
				
				if(event.getDbid()!=null && event.getDbid()!=0) {
					if (LOG.isDebugEnabled()) LOG.debug("Sending Event to ES:"+event.toString());
					// Send the event to the event forwarder
					eventIndex = populateEventIndexBodyFromEvent(event, EVENT_INDEX_NAME, EVENT_INDEX_TYPE);
					eventIndexresult = getJestClient().execute(eventIndex);
					
					if(LOG.isDebugEnabled()) {
						if (eventIndexresult==null) {
							LOG.debug("returned dresult==null");
						} else{
							LOG.debug("event sent to es index:"+EVENT_INDEX_NAME+" type:"+ EVENT_INDEX_TYPE+" received search dresult: "+eventIndexresult.getJsonString()
							+ "\n   response code:" +eventIndexresult.getResponseCode() 
							+ "\n   error message: "+eventIndexresult.getErrorMessage());
						}
					}
					
					
				} else {
					if (LOG.isDebugEnabled()) LOG.debug("Not Sending Event to ES: event.getDbid()="+event.getDbid()+ " Event="+event.toString());
				}

			}
			

		} catch (Exception ex){
			LOG.error("problem sending event to Elastic Search",ex);
		}

	}

	/**
	 * utility method to populate a Map with the most import event attributes
	 *
	 * @param body the map
	 * @param event the event object
	 */
	public Index populateEventIndexBodyFromEvent( Event event, String indexName, String indexType) {

		Map<String,String> body=new HashMap<String,String>();

		String id=(event.getDbid()==null ? null: Integer.toString(event.getDbid()));

		body.put("id",id);
		body.put("eventuei",event.getUei());

		Calendar cal=Calendar.getInstance();
		if (event.getCreationTime()==null) {
			if(LOG.isDebugEnabled()) LOG.debug("no event creation time for event.toString: "+ event.toString());
			cal.setTime(new Date());

		} else 	cal.setTime(event.getCreationTime()); // javax.xml.bind.DatatypeConverter.parseDateTime("2010-01-01T12:00:00Z");

		body.put("@timestamp", DatatypeConverter.printDateTime(cal));

		body.put("dow", Integer.toString(cal.get(Calendar.DAY_OF_WEEK)));
		body.put("hour",Integer.toString(cal.get(Calendar.HOUR_OF_DAY)));
		body.put("dom", Integer.toString(cal.get(Calendar.DAY_OF_MONTH))); 
		body.put("eventsource", event.getSource());
		body.put("ipaddr", event.getInterfaceAddress()!=null ? event.getInterfaceAddress().toString() : null );
		body.put("servicename", event.getService());
		// params are exported as attributes, see below
		body.put("eventseverity_text", event.getSeverity());
		body.put("eventseverity", Integer.toString(OnmsSeverity.get(event.getSeverity()).getId()));

		if(isLogEventDescription()) {
			body.put("eventdescr", event.getDescr());
		}

		body.put("host",event.getHost());
		for(Parm parm : event.getParmCollection()) {
			body.put("p_" + parm.getParmName(), parm.getValue().getContent());
		}
		body.put("interface", event.getInterface());
		body.put("logmsg", ( event.getLogmsg()!=null ? event.getLogmsg().getContent() : null ));
		body.put("logmsgdest", ( event.getLogmsg()!=null ? event.getLogmsg().getDest() : null ));

		body.put("nodeid", Long.toString(event.getNodeid()));

		// add node details
		if (nodeCache!=null){
			Map nodedetails = nodeCache.getEntry(event.getNodeid());
			for (Object key: nodedetails.keySet()){
				String keyStr = (String) key;
				String value = (String) nodedetails.get(key);
				body.put(keyStr, value);
			}
		}


		Index index = new Index.Builder(body).index(indexName)
				.type(indexType).id(id).build();

		return index;
	}

	/**
	 * An alarm change event will have a payload corresponding to the json representation of the
	 * Alarms table row for this alarm id. Both "oldalarmvalues" and "newalarmvalues" params may be populated
	 * The alarm index body will be populated with the "newalarmvalues" but if "newalarmvalues" is null then the
	 * "oldalarmvalues" json string will be used
	 * If cannot parse event into alarm then null index is returned
	 * @param body
	 * @param event
	 */
	public Index populateAlarmIndexBodyFromAlarmChangeEvent(Event event, String indexName, String indexType) {

		Map<String,String> body = new HashMap<String,String>();

		//get alarm change params from event
		Map<String,String> parmsMap = new HashMap<String,String>();
		for(Parm parm : event.getParmCollection()) {
			parmsMap.put( parm.getParmName(), parm.getValue().getContent());
		}

		String oldValues=parmsMap.get(OLD_ALARM_VALUES);
		String newValues=parmsMap.get(NEW_ALARM_VALUES);

		String payload=null;
		JSONObject alarmValues=new JSONObject() ;
		if (newValues!=null || oldValues!=null){
			try {
				JSONParser parser = new JSONParser();
				Object obj;

				if (newValues!=null) {
					payload = newValues;
				} else {
					payload = oldValues;
				}
				obj = parser.parse(payload);
				alarmValues = (JSONObject) obj;
				LOG.debug("payload alarmvalues.toString():" + alarmValues.toString());

			} catch (ParseException e1) {
				LOG.error("cannot parse event payload to json object. payload="+ payload, e1);
				return null;
			}

		}

		for (Object x: alarmValues.keySet()){
			String key=(String) x;
			String value = (alarmValues.get(key)==null) ? null : alarmValues.get(key).toString();

			if ("eventparms".equals(key) && value!=null){
				//decode event parms into alarm record
				List<Parm> params = EventParameterUtils.decode(value);
				for(Parm parm : params) {
					body.put("p_" + parm.getParmName(), parm.getValue().getContent());
				}
			} else{
				body.put(key, value);
			}

		}

		// add node details
		if (nodeCache!=null && event.getNodeid()!=null){
			Map nodedetails = nodeCache.getEntry(event.getNodeid());
			for (Object key: nodedetails.keySet()){
				String keyStr = (String) key;
				String value = (String) nodedetails.get(key);
				body.put(keyStr, value);
			}
		}

		Index index=null;

		if (alarmValues.get("alarmid")==null){
			LOG.error("No alarmid param - cannot create alarm logstash record from event content:"+ event.toString());
		} else{
			String id = alarmValues.get("alarmid").toString();

			index = new Index.Builder(body).index(indexName)
					.type(indexType).id(id).build();
		}

		return index;
	}

	private void maybeRefreshCache(Event event) {
		String uei=event.getUei();
		if(uei!=null && uei.startsWith("uei.opennms.org/nodes/")) {
			if (
					uei.endsWith("Added")
					|| uei.endsWith("Deleted")
					|| uei.endsWith("Updated")
					|| uei.endsWith("Changed")
					) {
				nodeCache.refreshEntry(event.getNodeid());
			}
		}
	}




}
