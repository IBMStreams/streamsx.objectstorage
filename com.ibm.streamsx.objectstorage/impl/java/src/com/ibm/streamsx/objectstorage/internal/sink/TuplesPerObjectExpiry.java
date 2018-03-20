package com.ibm.streamsx.objectstorage.internal.sink;

import org.ehcache.ValueSupplier;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;


public class TuplesPerObjectExpiry implements Expiry<Object, Object> {

	private int fTuplesPerObject;

	public TuplesPerObjectExpiry(int tuplesPerObject) {
		fTuplesPerObject = tuplesPerObject;
	}

	@Override
	public Duration getExpiryForCreation(Object paramK, Object paramV) {
		// the mapping will never expire
		return Duration.INFINITE;
	}

	@Override
	public Duration getExpiryForAccess(Object paramK, ValueSupplier<? extends Object> paramValueSupplier) {
		OSObject value = (OSObject)paramValueSupplier.value();
		
		//if (value.fDataBuffer.size() >= fTuplesPerObject) {
		if (value.fDataBufferCount >= fTuplesPerObject) {
//		System.out.println("getExpiryForAccess -> value.fTestDataBuffer.size(): " + value.fTestDataBuffer.size());
//		System.out.println("getExpiryForAccess -> fTuplesPerObject: " + fTuplesPerObject);
//		if (value.fTestDataBuffer.size() >= fTuplesPerObject) {
			// threshold has been reached - immediately expire
			return Duration.ZERO; 
		}
	    // number of tuples in the current object is still lower than
		// threshold - do not touch the entry
		return null;
	}

	@Override
	public Duration getExpiryForUpdate(Object paramK, ValueSupplier<? extends Object> paramValueSupplier,
			Object paramV) {
		OSObject value = (OSObject)paramValueSupplier.value();
//		System.out.println("getExpiryForUpdate -> value.fTestDataBuffer.size(): " + value.fTestDataBuffer.size());
//		System.out.println("getExpiryForUpdate -> fTuplesPerObject: " + fTuplesPerObject);
		
		if (value.fDataBufferCount >= fTuplesPerObject) {
//		if (value.fTestDataBuffer.size() >= fTuplesPerObject) {
			// threshold has been reached - immediately expire
			return Duration.ZERO; 
		}
	    // number of tuples in the current object is still lower than
		// threshold - do not touch the entry
		return null;
	}
}
