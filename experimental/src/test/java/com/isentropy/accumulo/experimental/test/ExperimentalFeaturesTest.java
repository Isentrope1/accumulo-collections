package com.isentropy.accumulo.experimental.test;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.io.DoubleBinarySerde;
import com.isentropy.accumulo.collections.io.LongBinarySerde;
import com.isentropy.accumulo.util.Util;

import static com.isentropy.accumulo.experimental.mappers.JavaScriptMappers.jsFilter;
import static com.isentropy.accumulo.experimental.mappers.JavaScriptMappers.jsTransform;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class ExperimentalFeaturesTest 
extends TestCase
{
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public ExperimentalFeaturesTest( String testName )
	{
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite( ExperimentalFeaturesTest.class );
	}

	public void testApp(){
		try{
			Connector c = new MockInstance().getConnector("root", new PasswordToken());
	    	//set up map load [x,2*x] for x in 1 to 1000
			AccumuloSortedMap asm = new AccumuloSortedMap(c,"mytable");
	    	asm.setKeySerde(new LongBinarySerde()).setValueSerde(new DoubleBinarySerde());
			for(long i=0;i<1000;i++){
				asm.put(i, 2*i);
			}
			asm.deriveMap(jsFilter("k % 2 == 0")).deriveMap(jsTransform("v*5")).sample(.1).dump(System.out);
		}
		
		catch(Exception e){
			e.printStackTrace();
			fail();
		}

	}
}

