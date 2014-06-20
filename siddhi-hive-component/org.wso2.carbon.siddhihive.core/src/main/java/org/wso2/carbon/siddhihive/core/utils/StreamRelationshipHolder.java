package org.wso2.carbon.siddhihive.core.utils;

import org.wso2.carbon.siddhihive.core.headerprocessor.LengthWindowStreamHandler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by firzhan on 6/18/14.
 */
public class StreamRelationshipHolder {

    private ConcurrentMap<String, String> parentChildHolderMap = null;

//    public StreamRelationshipHolder(){
//
//        parentChildHolderMap = new ConcurrentHashMap<String, String>();
//
//    }
//
//    public void addParentChildStream(String child, String parent){
//        parentChildHolderMap.put(child, parent);
//    }
//
//    public String getLastParentOfChain(String child, boolean onlyImmediateParent){
//
//        if(onlyImmediateParent)
//            return parentChildHolderMap.get(child);
//
//        String lastParent ="";
//
//        while(parentChildHolderMap.ge)
//    }


}
