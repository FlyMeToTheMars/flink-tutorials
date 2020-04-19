package org.apache.flink.test.streaming.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Thread-safe sink for collecting elements into an on-heap list.
 *
 * @param <T> element type
 */
public class TestListResultSink<T> extends RichSinkFunction<T> {

	private static final long serialVersionUID = 1L;
	private int resultListId;

	public TestListResultSink() {
		this.resultListId = TestListWrapper.getInstance().createList();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

	@Override
	public void invoke(T value) throws Exception {
		synchronized (resultList()) {
			resultList().add(value);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@SuppressWarnings("unchecked")
	private List<T> resultList() {
		synchronized (TestListWrapper.getInstance()) {
			return (List<T>) TestListWrapper.getInstance().getList(resultListId);
		}
	}

	public List<T> getResult() {
		synchronized (resultList()) {
			ArrayList<T> copiedList = new ArrayList<T>(resultList());
			return copiedList;
		}
	}

	public List<T> getSortedResult() {
		synchronized (resultList()) {
			ArrayList<T> sortedList = new ArrayList<T>(resultList());
			Collections.sort((List) sortedList);
			return sortedList;
		}
	}
}