/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.util;

import java.util.function.Function;

/** Immutable class for measuring time durations in milliseconds.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */
public class TimeCounter {

	
	public static TimeCounter INTANCE = new TimeCounter();
	  
	  
	/** Returns the measured time in milliseconds.
	 *
	 * @return the measured time in milliseconds
	 */
	public Long time(){throw new UnsupportedOperationException();};

	/** Returns a new [[StoppedTimeCounter]].
	 *
	 * @return a new stopped time counter
	 */
	public StoppedTimeCounter create(){
		return new StoppedTimeCounter(0L);
	}

	/** Returns a new [[StartedTimeCounter]].
	 *
	 * @return a new started time counter
	 */
	public StartedTimeCounter start(){
		return create().start();
	}

	/** Runs the specified closure and returns a stopped time counter measuring its execution time.
	 * @param <StoppedTimeCounter>
	 *
	 * @param f the closure to be run and measured
	 * @return a new stopped time counter
	 */
	public StoppedTimeCounter apply(Function f) {
		StartedTimeCounter counter = create().start();
		f.apply(null);
		return counter.stop();
	}


	/** A started [[TimeCounter]].
	 *
	 * @param startTime the start time in milliseconds
	 * @param runTime   the already run time in milliseconds
	 */
	public class StartedTimeCounter extends TimeCounter {

		Long startTime;
		Long runTime;

		public StartedTimeCounter(Long startTime, Long runTime) {
			this.startTime=startTime;
			this.runTime=runTime;
		}
		/** @inheritdoc */


		/** @inheritdoc */
		@Override 
		public Long time(){
			return runTime + System.currentTimeMillis() - startTime;
		}

		/** Returns a new stopped time counter.
		 *
		 * @return a new stopped time counter
		 */
		public StoppedTimeCounter stop(){
			return new StoppedTimeCounter(time());
		}

		public String toString() {
			return time()+" ms";
		};

	}

	/** A stopped [[TimeCounter]].
	 *
	 * @param runTime the total run time in milliseconds
	 */
	public class StoppedTimeCounter extends TimeCounter {


		Long runTime;

		public StoppedTimeCounter(Long runTime) {
			this.runTime=runTime;
		}


		/** @inheritdoc */
		@Override 
		public Long time(){
			return this.runTime;
		}

		/** Returns a new started time counter.
		 *
		 * @return a new started time counter
		 */
		public StartedTimeCounter start(){
			return  new StartedTimeCounter(System.currentTimeMillis(), time());
		}

		public String toString() {
			return time()+" ms";
		};
	}

}
