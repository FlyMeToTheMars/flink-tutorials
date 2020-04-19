package org.apache.flink.test.streaming.runtime.util;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Identity mapper.
 */
public class NoOpIntMap implements MapFunction<Integer, Integer> {
	private static final long serialVersionUID = 1L;

	public Integer map(Integer value) throws Exception {
		return value;
	}
}
