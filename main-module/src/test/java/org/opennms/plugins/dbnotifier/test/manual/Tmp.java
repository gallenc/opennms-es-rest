package org.opennms.plugins.dbnotifier.test.manual;

import static org.junit.Assert.*;

import org.junit.Test;
import org.opennms.plugins.elasticsearch.rest.EventToIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tmp {
	private static final Logger LOG = LoggerFactory.getLogger(Tmp.class);

	@Test
	public void test() {
		System.out.println("start of test alarmEventToIndex");
		LOG.debug("debug start of test alarmEventToIndex");

		//EventToIndex e2e= new EventToIndex();
		System.out.println("end of test alarmEventToIndex");
	}

}
