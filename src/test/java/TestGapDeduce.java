/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestGapDeduce {

  private MapDriver<Text, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;

  @Before
  public void setUp() {
    Gapper mapper = new Gapper();
    Deducer reducer = new Deducer();
    mapDriver = MapDriver.newMapDriver(mapper);;
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
  }

  @Test
  public void testMapper() {
    mapDriver.withInput(new Text("sanford"), new Text("sage"));
    mapDriver.withOutput(new Text("sage"), new Text("sanford"));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("sage"));
    values.add(new Text("ian"));
    values.add(new Text("sage"));
    reduceDriver.withInput(new Text("sanford"), values);
    reduceDriver.withOutput(new Text("sanford"), 
        new Text("[ian, sage]"));
    reduceDriver.runTest();
  }
}
