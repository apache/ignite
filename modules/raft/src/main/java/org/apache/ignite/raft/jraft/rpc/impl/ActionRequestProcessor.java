/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc.impl;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.ErrorResponseBuilder;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.util.BytesUtil;
import org.apache.ignite.raft.jraft.util.JDKMarshaller;

/**
 * Process action request.
 */
public class ActionRequestProcessor implements RpcProcessor<ActionRequest> {
    private final Executor executor;

    private final RaftMessagesFactory factory;

    public ActionRequestProcessor(Executor executor, RaftMessagesFactory factory) {
        this.executor = executor;
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override public void handleRequest(RpcContext rpcCtx, ActionRequest request) {
        Node node = rpcCtx.getNodeManager().get(request.groupId(), new PeerId(rpcCtx.getLocalAddress()));

        if (node == null) {
            rpcCtx.sendResponse(factory.errorResponse().errorCode(RaftError.UNKNOWN.getNumber()).build());

            return;
        }

        if (request.command() instanceof WriteCommand) {
            node.apply(new Task(ByteBuffer.wrap(JDKMarshaller.DEFAULT.marshall(request.command())),
                new CommandClosureImpl<>(request.command()) {
                    @Override public void result(Serializable res) {
                        rpcCtx.sendResponse(factory.actionResponse().result(res).build());
                    }

                    @Override public void run(Status status) {
                        assert !status.isOk() : status;

                        sendError(rpcCtx, status, node);
                    }
                }));
        }
        else {
            if (request.readOnlySafe()) {
                node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                    @Override public void run(Status status, long index, byte[] reqCtx) {
                        if (status.isOk()) {
                            JRaftServerImpl.DelegatingStateMachine fsm =
                                (JRaftServerImpl.DelegatingStateMachine) node.getOptions().getFsm();

                            try {
                                fsm.getListener().onRead(List.<CommandClosure<ReadCommand>>of(new CommandClosure<>() {
                                    @Override public ReadCommand command() {
                                        return (ReadCommand)request.command();
                                    }

                                    @Override public void result(Serializable res) {
                                        rpcCtx.sendResponse(factory.actionResponse().result(res).build());
                                    }
                                }).iterator());
                            }
                            catch (Exception e) {
                                sendError(rpcCtx, RaftError.ESTATEMACHINE, e.getMessage());
                            }
                        }
                        else
                            sendError(rpcCtx, status, node);
                    }
                });
            }
            else {
                // TODO asch remove copy paste, batching https://issues.apache.org/jira/browse/IGNITE-14832
                JRaftServerImpl.DelegatingStateMachine fsm =
                    (JRaftServerImpl.DelegatingStateMachine) node.getOptions().getFsm();

                try {
                    fsm.getListener().onRead(List.<CommandClosure<ReadCommand>>of(new CommandClosure<>() {
                        @Override public ReadCommand command() {
                            return (ReadCommand)request.command();
                        }

                        @Override public void result(Serializable res) {
                            rpcCtx.sendResponse(factory.actionResponse().result(res).build());
                        }
                    }).iterator());
                }
                catch (Exception e) {
                    sendError(rpcCtx, RaftError.ESTATEMACHINE, e.getMessage());
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String interest() {
        return ActionRequest.class.getName();
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return executor;
    }

    /**
     * @param ctx Context.
     * @param error RaftError code.
     * @param msg Message.
     */
    private void sendError(RpcContext ctx, RaftError error, String msg) {
        RpcRequests.ErrorResponse resp = factory.errorResponse()
            .errorCode(error.getNumber())
            .errorMsg(msg)
            .build();

        ctx.sendResponse(resp);
    }

    /**
     * @param ctx The context.
     * @param status The status.
     * @param node Raft node.
     */
    private void sendError(RpcContext ctx, Status status, Node node) {
        RaftError raftError = status.getRaftError();

        Message response;

        if (raftError == RaftError.EPERM && node.getLeaderId() != null)
            response = RaftRpcFactory.DEFAULT
                .newResponse(node.getLeaderId().toString(), factory, RaftError.EPERM, status.getErrorMsg());
        else
            response = RaftRpcFactory.DEFAULT
                .newResponse(factory, raftError, status.getErrorMsg());

        ctx.sendResponse(response);
    }

    /**
     *
     */
    private abstract static class CommandClosureImpl<T extends Command> implements Closure, CommandClosure<T> {
        /**
         *
         */
        private final T command;

        /**
         * @param command The command.
         */
        CommandClosureImpl(T command) {
            this.command = command;
        }

        /** {@inheritDoc} */
        @Override public T command() {
            return command;
        }
    }
}
