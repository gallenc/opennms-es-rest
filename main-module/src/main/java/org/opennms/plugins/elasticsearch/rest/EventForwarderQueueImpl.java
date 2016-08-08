package org.opennms.plugins.elasticsearch.rest;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.xml.event.Event;
import org.opennms.netmgt.xml.event.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * queues events received from OpenNMS for forwarding to elastic search.
 * drops events if ES not keeping up and queue has become full.  
 * @author admin
 *
 */
public class EventForwarderQueueImpl implements EventForwarder {

	private static final Logger LOG = LoggerFactory.getLogger(EventForwarderQueueImpl.class);

	private Integer maxQueueLength=1000;
	
	private LinkedBlockingQueue<Event> queue = null;
	private AtomicBoolean clientRunning = new AtomicBoolean(false);

	private RemovingConsumer removingConsumer = new RemovingConsumer();
	private Thread removingConsumerThread = new Thread(removingConsumer);
	
	private EventToIndex eventToIndex=null;
	
	public EventToIndex getEventToIndex() {
		return eventToIndex;
	}

	public void setEventToIndex(EventToIndex eventToIndex) {
		this.eventToIndex = eventToIndex;
	}
	
	public Integer getMaxQueueLength() {
		return maxQueueLength;
	}

	public void setMaxQueueLength(Integer maxQueueLength) {
		this.maxQueueLength = maxQueueLength;
	}
	
	@Override
	public void sendNow(Event event) {		
		if (LOG.isDebugEnabled()) LOG.debug("Event received: queue.size() "+queue.size()
				+ " queue.remainingCapacity() "+ queue.remainingCapacity()
				+ "\n   event:" + event.toString());
		if (! queue.offer(event)){
			LOG.warn("Cannot queue any more events. Event queue full. size="+queue.size());
		};
	}

	@Override
	public void sendNow(Log arg0) {
		// NOT USED
	}
	
	public void init(){
		LOG.debug("initialising EventFowarderQueue with queue size "+maxQueueLength);
		queue =  new LinkedBlockingQueue<Event>(maxQueueLength);

		// start consuming thread
		clientRunning.set(true);
		removingConsumerThread.start();

	}

	public void destroy(){
		LOG.debug("shutting down EventFowarderQueue");

		// signal consuming thread to stop
		clientRunning.set(false);
		removingConsumerThread.interrupt();
	}
	
	/*
	 * Class run in separate thread to remove and process notifications from the queue 
	 */
	private class RemovingConsumer implements Runnable {
		//TODO remove final Logger LOG = LoggerFactory.getLogger(EventForwarderQueueImpl.class);

		@Override
		public void run() {

			// we remove elements from the queue until interrupted and clientRunning==false.
			while (clientRunning.get()) {
				try {
					Event event = queue.take();

					if(LOG.isDebugEnabled()) LOG.debug("Event received from queue by consumer thread :\n event:"+event.toString());
					
					// send event to index processor
					if (eventToIndex!=null){
						eventToIndex.forwardEvent(event);
					}else {
						LOG.error("cannot send event eventToIndex is null");
					}

				} catch (InterruptedException e) { }

			}

			LOG.debug("shutting down event consumer thread");
		}
	}

}
