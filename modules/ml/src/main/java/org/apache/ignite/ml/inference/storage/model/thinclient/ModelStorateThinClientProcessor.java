/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.inference.storage.model.thinclient;

import java.util.Arrays;
import java.util.Set;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientBooleanResponse;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.CustomQueryProcessor;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;

/**
 * Processor for model storage commands in thin client.
 */
public class ModelStorateThinClientProcessor implements CustomQueryProcessor {
    /**
     * Operations of model storage for GGFS client.
     */
    public enum Method {
        /** */WRITE_FILE(0),
        /** */READ_FILE(1),
        /** */MOVE(2),
        /** */STAT(3),
        /** */EXISTS(4),
        /** */REMOVE(5),
        /** */MKDIR(6),
        /** */MKDIRS(7),
        /** */LIST_FILES(8);

        /** Operaion id. */
        private final int id;

        /**
         * Create an instance of Operaion.
         *
         * @param id Id of operation.
         */
        Method(int id) {
            this.id = id;
        }

        /**
         * Resolve operation instance by id.
         *
         * @param id Operation Id.
         * @return Operation.
         */
        static Method find(int id) {
            for (Method op : Method.values()) {
                if (op.id == id)
                    return op;
            }

            return null;
        }

        /**
         * @return Id of method.
         */
        public byte id() {
            return (byte)id;
        }
    }

    /** Processor id. */
    public static final String PROCESSOR_ID = "ML_MODEL_STORAGE";

    /** Model storage. */
    private final ModelStorage modelStorage;

    /**
     * Creates an instance of model storage commands processor.
     *
     * @param modelStorage Model storage.
     */
    public ModelStorateThinClientProcessor(ModelStorage modelStorage) {
        this.modelStorage = modelStorage;
    }

    /** {@inheritDoc} */
    @Override public ClientResponse call(long requestId, byte methodId, BinaryRawReader reader) {
        Method op = Method.find(methodId);
        if (op == null)
            return error(requestId, "Operation was not found [id=" + methodId + "]");

        switch (op) {
            case WRITE_FILE:
                return writeFile(requestId, reader);
            case READ_FILE:
                return readFile(requestId, reader);
            case MOVE:
                return moveFile(requestId, reader);
            case STAT:
                return getFileStat(requestId, reader);
            case EXISTS:
                return isExists(requestId, reader);
            case REMOVE:
                return remove(requestId, reader);
            case MKDIR:
                return mkdir(requestId, reader);
            case MKDIRS:
                return mkdirs(requestId, reader);
            case LIST_FILES:
                return listFiles(requestId, reader);
        }

        throw new IllegalArgumentException("Cannot find handler for operation [id=" + op.name() + "]");
    }

    /**
     * Writes file to model storate.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse writeFile(long reqId, BinaryRawReader reader) {
        String path = reader.readString();

        return modelStorage.lockPaths(() -> {
            boolean create = reader.readBoolean();
            boolean append = reader.readBoolean();
            byte[] newData = reader.readByteArray();

            boolean fileAlreadyExists = modelStorage.exists(path);
            if (!create && !fileAlreadyExists)
                return error(reqId, "File doesn't exist [path=" + path + "]");

            byte[] currentData = new byte[0];
            if (fileAlreadyExists) {
                if (append && !create)
                    currentData = modelStorage.getFile(path);
                modelStorage.remove(path);
            }

            byte[] result = new byte[currentData.length + newData.length];
            System.arraycopy(currentData, 0, result, 0, currentData.length);
            System.arraycopy(newData, 0, result, currentData.length, newData.length);
            modelStorage.putFile(path, result);

            return new ClientResponse(reqId);
        }, path);
    }

    /**
     * Reads file from model storage.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse readFile(long reqId, BinaryRawReader reader) {
        String path = reader.readString();

        return modelStorage.lockPaths(() -> {
            if (!modelStorage.exists(path))
                return error(reqId, "File not found [path=" + path + "]");

            if (!modelStorage.isFile(path))
                return error(reqId, "File is not regular file [path" + path + "]");

            return new FileRespose(reqId, modelStorage.getFile(path));
        }, path);
    }

    /**
     * Moves file in model storage.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse moveFile(long reqId, BinaryRawReader reader) {
        String from = reader.readString();
        String to = reader.readString();

        String[] pathsToLock = new String[] {from, to};
        Arrays.sort(pathsToLock, (s1, s2) -> {
            if (s1.length() == s2.length())
                return s1.compareTo(s2);
            else
                return Integer.compare(s1.length(), s2.length());
        });

        return modelStorage.lockPaths(() -> {
            if (!modelStorage.exists(from))
                return error(reqId, "File not found [path=" + from + "]");
            if (modelStorage.exists(to))
                return error(reqId, "File already exists [path=" + to + "]");
            if (!modelStorage.isFile(from))
                return error(reqId, "File is not regular file [path=" + from + "]");

            byte[] file = modelStorage.getFile(from);
            modelStorage.remove(from);
            modelStorage.putFile(to, file);
            return new ClientResponse(reqId);
        }, pathsToLock);
    }

    /**
     * Returns statistics about file.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse getFileStat(long reqId, BinaryRawReader reader) {
        String path = reader.readString();

        return modelStorage.lockPaths(() -> {
            if (!modelStorage.exists(path))
                return error(reqId, "File not found [path=" + path + "]");

            return new FileStatResponse(reqId, modelStorage.getFileStat(path));
        }, path);
    }

    /**
     * Checks file existance.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse isExists(long reqId, BinaryRawReader reader) {
        String path = reader.readString();
        return new ClientBooleanResponse(reqId, modelStorage.exists(path));
    }

    /**
     * Removes file in model storage.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse remove(long reqId, BinaryRawReader reader) {
        String path = reader.readString();

        return modelStorage.lockPaths(() -> {
            if (modelStorage.exists(path))
                try {
                    modelStorage.remove(path);
                }
                catch (IllegalArgumentException e) {
                    return error(reqId, "Cannot delete non-empty directory [path= " + path + "]");
                }

            return new ClientResponse(reqId);
        }, path);
    }

    /**
     * Creates directory in model storage.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse mkdir(long reqId, BinaryRawReader reader) {
        String path = reader.readString();
        boolean onlyIfNotExists = reader.readBoolean();

        return modelStorage.lockPaths(() -> {
            if (onlyIfNotExists && modelStorage.exists(path))
                return error(reqId, "Directory already exists [path=" + path + "]");

            modelStorage.mkdir(path, onlyIfNotExists);
            return new ClientResponse(reqId);
        }, path);
    }

    /**
     * Creates directories in model storage in recursive manner.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse mkdirs(long reqId, BinaryRawReader reader) {
        String path = reader.readString();
        boolean onlyIfNotExists = reader.readBoolean();

        return modelStorage.lockPaths(() -> {
            if (onlyIfNotExists && modelStorage.exists(path))
                return error(reqId, "Directory already exists [path=" + path + "]");

            modelStorage.mkdirs(path);
            return new ClientResponse(reqId);
        }, path);
    }

    /**
     * Returns list of files in model storage.
     *
     * @param reqId Request id.
     * @param reader Reader.
     * @return Response.
     */
    private ClientResponse listFiles(long reqId, BinaryRawReader reader) {
        String path = reader.readString();

        return modelStorage.lockPaths(() -> {
            if (!modelStorage.exists(path))
                return error(reqId, "Direcrory not found [path=" + path + "]");

            if (modelStorage.isFile(path))
                return error(reqId, "Specified path is not associated with directory [path=" + path + "]");

            Set<String> files = modelStorage.listFiles(path);
            return new FilesListResponse(reqId, files);
        }, path);
    }

    /**
     * Compose error rensponse for client.
     *
     * @param requestId Request id.
     * @param msg Error message.
     * @return Response.
     */
    private ClientResponse error(long requestId, String msg) {
        return new ClientResponse(requestId, msg);
    }

    /** {@inheritDoc} */
    @Override public String id() {
        return PROCESSOR_ID;
    }
}
