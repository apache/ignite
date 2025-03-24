/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.igfs;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.processors.igfs.client.*;
import org.apache.ignite.internal.processors.igfs.client.meta.*;
import org.apache.ignite.internal.processors.igfs.data.IgfsDataPutProcessor;
import org.apache.ignite.internal.processors.igfs.meta.*;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;

/**
 * Binary context.
 */
public class IgfsMessageFactoryProvider implements MessageFactoryProvider {
   

    /** Set of system classes that should be marshalled with BinaryMarshaller. */
    private static final Set<String> BINARYLIZABLE_SYS_CLSS = BinaryContext.BINARYLIZABLE_SYS_CLSS;;

    /* Binarylizable system classes set initialization. */
    static {
        Set<String> sysClss = new HashSet<>();

        // IGFS classes.
        sysClss.add(IgfsPath.class.getName());

        sysClss.add(IgfsBlockKey.class.getName());
        sysClss.add(IgfsDirectoryInfo.class.getName());
        sysClss.add(IgfsFileAffinityRange.class.getName());
        sysClss.add(IgfsFileInfo.class.getName());
        sysClss.add(IgfsFileMap.class.getName());
        sysClss.add(IgfsListingEntry.class.getName());

        sysClss.add(IgfsDataPutProcessor.class.getName());

        sysClss.add(IgfsMetaDirectoryCreateProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingAddProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingRemoveProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingRenameProcessor.class.getName());
        sysClss.add(IgfsMetaDirectoryListingReplaceProcessor.class.getName());
        sysClss.add(IgfsMetaFileCreateProcessor.class.getName());
        sysClss.add(IgfsMetaFileLockProcessor.class.getName());
        sysClss.add(IgfsMetaFileRangeDeleteProcessor.class.getName());
        sysClss.add(IgfsMetaFileRangeUpdateProcessor.class.getName());
        sysClss.add(IgfsMetaFileReserveSpaceProcessor.class.getName());
        sysClss.add(IgfsMetaFileUnlockProcessor.class.getName());
        sysClss.add(IgfsMetaUpdatePropertiesProcessor.class.getName());
        sysClss.add(IgfsMetaUpdateTimesProcessor.class.getName());

        sysClss.add(IgfsClientMetaIdsForPathCallable.class.getName());
        sysClss.add(IgfsClientMetaInfoForPathCallable.class.getName());
        sysClss.add(IgfsClientMetaUnlockCallable.class.getName());

        sysClss.add(IgfsClientAffinityCallable.class.getName());
        sysClss.add(IgfsClientDeleteCallable.class.getName());
        sysClss.add(IgfsClientExistsCallable.class.getName());
        sysClss.add(IgfsClientInfoCallable.class.getName());
        sysClss.add(IgfsClientListFilesCallable.class.getName());
        sysClss.add(IgfsClientListPathsCallable.class.getName());
        sysClss.add(IgfsClientMkdirsCallable.class.getName());
        sysClss.add(IgfsClientRenameCallable.class.getName());
        sysClss.add(IgfsClientSetTimesCallable.class.getName());
        sysClss.add(IgfsClientSizeCallable.class.getName());
        sysClss.add(IgfsClientSummaryCallable.class.getName());
        sysClss.add(IgfsClientUpdateCallable.class.getName());
        
        BINARYLIZABLE_SYS_CLSS.addAll(sysClss);
        
    }

	@Override
	public void registerAll(MessageFactory factory) {
		
		factory.register((short)64, IgfsAckMessage::new);
    	factory.register((short)65, IgfsBlockKey::new);
    	factory.register((short)66, IgfsBlocksMessage::new);
    	factory.register((short)67, IgfsDeleteMessage::new);
    	factory.register((short)68, IgfsFileAffinityRange::new);
    	factory.register((short)69, IgfsFragmentizerRequest::new);
    	factory.register((short)70, IgfsFragmentizerResponse::new);
    	factory.register((short)71, IgfsSyncMessage::new);
		
	}
  
}
