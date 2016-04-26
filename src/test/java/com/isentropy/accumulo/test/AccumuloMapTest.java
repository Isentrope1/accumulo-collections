/* 
Accumulo Collections
Copyright 2016 Isentropy LLC
Written by Jonathan Wolff <jwolff@isentropy.com>
Isentropy specializes in big data and quantitative programming consulting,
with particular expertise in Accumulo development and installation. 
More info at http://isentropy.com.


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.isentropy.accumulo.test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.MapAggregates;
import com.isentropy.accumulo.collections.io.DoubleBinarySerde;
import com.isentropy.accumulo.collections.io.LongBinarySerde;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AccumuloMapTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AccumuloMapTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AccumuloMapTest.class );
    }

    public void demo() throws AccumuloException, AccumuloSecurityException, InterruptedException{
    	//connect to accumulo
    	//Connector c = new ZooKeeperInstance(instanceName,zookeepers).getConnector(myUsername, new PasswordToken(myPassword));
    	Connector c = new MockInstance().getConnector("root", new PasswordToken());
    	//set up map load [x,2*x] for x in 1 to 1000
    	AccumuloSortedMap asm = new AccumuloSortedMap(c,"mytable");
    	asm.setKeySerde(new LongBinarySerde()).setValueSerde(new LongBinarySerde());
		for(long i=0;i<1000;i++){
			asm.put(i, 2*i);
		}
		
		// MAP FEATURES
		System.out.println("get(100) = "+asm.get(100));

		// SORTED MAP FEATURES
		AccumuloSortedMap submap = (AccumuloSortedMap) asm.subMap(10, 20);
		// should be null
		System.out.println("submap(10,20).get(100) = "+submap.get(100));
		//should be 10
		System.out.println("submap(10,20).firstKey() = "+submap.firstKey());
		
		// SAMPLING
		// sample 50% of submap and print key values
		submap.sample(.5).dump(System.out); System.out.println();
		// this should be a DIFFERENT set because the seed is random in both calls to sample()
		submap.sample(.5).dump(System.out); System.out.println();
		
		// sample 40% of entries based on the ordering of seed "randomseed"
		submap.sample(0,.4,"randomseed").dump(System.out); System.out.println();
		// should be the SAME set as above if map hasn't changed because seed and hash range are the same
		submap.sample(0,.4,"randomseed").dump(System.out); System.out.println();
		
		// SERVER-SIDE aggregates
		//numerical stats work for values that are instances of Number
		System.out.println("submap value mean = "+MapAggregates.valueStats(submap).getMean());
		//size() is also computed on tablet servers
		System.out.println("submap size = "+submap.size());
		
		//TIMEOUT features
		asm.setTimeOutMs(5000);
		Thread.sleep(3000);
		asm.put(123, 345);
		Thread.sleep(2000);
		//all entries except 123 will have timed out by now. size should be 1
		System.out.println("map size after sleep = "+asm.size());
		
    }
    
    public void testApp()
    {
    	try{
    		demo();
    		/*
    		long entries = 1000;
    		AccumuloSortedMap ssm = AccumuloSortedMap.getTestInstance();
    		ssm.setColumnVisibility("asd".getBytes());
			ssm.setKeySerde(new LongBinarySerde());
			for(long i=0;i<entries;i++){
				ssm.put(i, 2*i);
			}
			assertTrue(ssm.firstKey().equals(0l));
			
			System.out.println("firstKey = "+ssm.firstKey()+", size() = "+ssm.size());
			assertTrue(ssm.size() == entries);
			
			
			AccumuloSortedMap submap = (AccumuloSortedMap) ssm.subMap(10, 20);
			assertTrue(submap.size() == 10);
			Iterator it = submap.entrySet().iterator();
			for(;it.hasNext();){
				Entry e = (Entry) it.next();
				System.out.println("k = "+ e.getKey() + ", v = "+e.getValue());
			}
			it= submap.keySet().iterator();
			for(;it.hasNext();){
				System.out.println("k = "+ it.next());
			}
			it= submap.values().iterator();
			for(;it.hasNext();){
				System.out.println("v = "+ it.next());
			}
			StatisticalSummary ssmStats = MapAggregates.valueStats(ssm);
			System.out.println("ssm stats:\n"+ssmStats);
			System.out.println("submap stats:\n"+MapAggregates.valueStats(submap));
			assertTrue(ssmStats.getN() == 1000);
*/
    	}
    	catch(Exception e){
    		e.printStackTrace();
            fail();
    	}
    }
}
