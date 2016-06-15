%% Copyright (c) 2013 by Tiger Zhang. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

-module(msgbus_amqp_proxy_client).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-compile({parse_transform, lager_transform}).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {
    tref,
    is_unsubscribe,
    name,
    level,
    connection,
    channel,
    exchange,
    params,
    amqp_package_sent_count,
    amqp_package_recv_count,
    receiver_module,
    stat_module,
    consumer_stat,
    consumer_last_stat,
    consumer_check_interval,
    consumer_msg_rate,  %% the rate of consumer per second
    queue_info,
    priority
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, start_link/1, test/0, close/1, unsubscribe/0, subscribe/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_link({Id, Params, OutgoingQueues, IncomingQueues, NodeTag}) ->
    gen_server:start_link({local, Id}, ?MODULE, {Params, OutgoingQueues, IncomingQueues, NodeTag}, []).

receiver_module_name(Receiver) ->
    Receiver.

close(Id) ->
    gen_server:call(Id, close).

unsubscribe() ->
    gen_server:call(?MODULE, unsubscribe).

subscribe() ->
    gen_server:call(?MODULE, subscribe).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Params, OutgoingQueues, IncomingQueues, NodeTag}) ->
    {ok, Receiver} = application:get_env(receiver_module),
    Stat = case application:get_env(stat_module) of
               {ok, StatModule} ->
                   StatModule;
               Else ->
                   Else
           end,
    ConsumerStat = application:get_env(msgbus_amqp_proxy, consumer_stat, {undefined, undefined}),
    ConsumerCheckInterval = application:get_env(msgbus_amqp_proxy, consumer_check_interval, 2),
    MsgRate = application:get_env(msgbus_amqp_proxy, consumer_msg_rate, 500),

    Name = config_val(name, Params, ?MODULE),
    Level = config_val(level, Params, debug),
    Exchange = config_val(exchange, Params, list_to_binary(atom_to_list(?MODULE))),
    Priority = config_val(priority, Params, 0),

    AmqpParams = #amqp_params_network{
        username = config_val(amqp_user, Params, <<"guest">>),
        password = config_val(amqp_pass, Params, <<"guest">>),
        virtual_host = config_val(amqp_vhost, Params, <<"/">>),
        host = config_val(amqp_host, Params, "localhost"),
        port = config_val(amqp_port, Params, 5672)
    },

    lager:info("Connecting to: ~p", [Name]),

    {Connection2, Channel2, QueueInfo} =
        case amqp_channel(AmqpParams) of
            {ok, Connection, Channel} ->

                % declare exchange
                case amqp_channel:call(Channel,
                    #'exchange.declare'{exchange = Exchange, type = <<"topic">>}) of
                    #'exchange.declare_ok'{} ->
                        lager:info("declare exchange succeeded: ~p", [Exchange]);
                    Return ->
                        lager:error("declare exchange failed: ~p", [Return])
                end,

                % Subscribe incoming queues
                SubscribeInfo = [subscribe_incoming_queues(Key, Queue, Exchange, Channel, NodeTag) || {Key, Queue} <- IncomingQueues],
                lager:debug("Subscribe queue Info ~p", [SubscribeInfo]),

                declare_and_bind_outgoing_queues(Channel, Exchange, OutgoingQueues),

                GroupName = "msgbus_amqp_clients_" ++ integer_to_list(Priority),
                ets:insert_new(msgbus_amqp_clients_priority_table, {Priority, GroupName}),
                case pg2:get_members(GroupName) of
                    {error, {no_such_group, GroupName}} ->
                        pg2:create(GroupName);
                    _ ->
                        ignore
                end,
                pg2:join(GroupName, self()),

                {Connection, Channel, SubscribeInfo};
            _Error ->
                Interval = 10,
                lager:error("amqp_channel failed. will try again after ~p s", [Interval]),
                % exit the client after 10 seconds, let the supervisor recreate it
                timer:exit_after(timer:seconds(Interval), "Connect failed"),
                {undefined, undefined, []}
        end,

    ConsumerLastStat = case ConsumerStat of
                           {undefined, _} ->
                               0;
                           {Mod, Fn} ->
                               Mod:Fn()
    end,
    TRef = erlang:start_timer(ConsumerCheckInterval * 1000, self(), check_consumer),
    {ok, #state{
        name = Name,
        level = Level,
        tref = TRef,
        connection = Connection2,
        channel = Channel2,
        exchange = Exchange,
        params = AmqpParams,
        amqp_package_sent_count = 0,
        is_unsubscribe = false,
        amqp_package_recv_count = 0,
        receiver_module = receiver_module_name(Receiver),
        stat_module = receiver_module_name(Stat),
        consumer_last_stat = ConsumerLastStat,
        consumer_check_interval = ConsumerCheckInterval,
        consumer_msg_rate = MsgRate,
        consumer_stat = ConsumerStat,
        queue_info = QueueInfo,
        priority = Priority
    }}.

handle_call(unsubscribe, _From,  #state{channel = Channel, queue_info = QueueInfo, is_unsubscribe = IsUnSubScribe} = State) ->
    case IsUnSubScribe of
        false ->
            unsubscribe_incomming_queues(Channel, QueueInfo);
        _ ->
            ignore
    end,
    State2 = State#state{is_unsubscribe = disable}, %% set flag disable to make sure won't subscribe queue
    {reply, ok, State2};

handle_call(subscribe, _From,  #state{channel = Channel,
    is_unsubscribe = IsUnsubscribe,
    queue_info = QueueInfo} = State) ->
    State2 = case IsUnsubscribe of
                 false ->
                     State;
                 _ ->
                      NewQueueInfo = [{
                              ConsumeQueue,
                              amqp_channel:subscribe(Channel, #'basic.consume'{queue = ConsumeQueue,
                                  consumer_tag = ConsumerTag, no_ack = true}, self())
                          }  || {ConsumeQueue, {_, ConsumerTag}} <- QueueInfo],
                lager:debug("Resume Consumer \n old info ~p \n new info ~p", [QueueInfo, NewQueueInfo]),
                State#state{is_unsubscribe = false, queue_info=NewQueueInfo}
    end,
    {reply, ok, State2};

handle_call(close, _From, #state{connection = Connection, channel = Channel} = State) ->
    lager:debug("~p", [<<"close channel">>]),
    amqp_channel:close(Channel),
    lager:debug("~p", [<<"close connection">>]),
    amqp_connection:close(Connection),
    {reply, ok, State};

handle_call({forward_to_amqp, RoutingKey, Message}, _From,
    #state{params = AmqpParams, exchange = Exchange, amqp_package_sent_count = Sent} = State) ->
    State2 = case amqp_channel(AmqpParams) of
                 {ok, Connection, Channel} ->
                     amqp_publish(Exchange, RoutingKey, Message, Channel, State),
                     State#state{amqp_package_sent_count = Sent + 1, connection = Connection, channel = Channel};
                 _ ->
                     State
             end,
    {reply, ok, State2};

handle_call({declare_bind, RoutingKey, Queue}, _From, #state{params = AmqpParams, exchange = Exchange} = State) ->
    case amqp_channel(AmqpParams) of
        {ok, _Connection, Channel} ->
            #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
            Binding = #'queue.bind'{queue = Queue,
                exchange = Exchange,
                routing_key = RoutingKey},
            #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding);
        _ ->
            State
    end,
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, Type, Pid, Info}, State = #state{priority = Priority}) ->
    lager:info("DOWN: ~p", [{Ref, Type, Pid, Info}]),

    GroupName = "msgbus_amqp_clients_" ++ integer_to_list(Priority),
    MyPid = self(),
    case pg2:get_members(GroupName) of
        [MyPid] ->
            ets:match_delete(msgbus_amqp_clients_priority_table, {'_', GroupName}),
            pg2:delete(GroupName);
        _ ->
            pg2:leave(GroupName, self())
    end,

    % exit the client after 10 seconds, let the supervisor recreate it
    timer:exit_after(timer:seconds(10), "Connection closed"),

    {noreply, State};
handle_info(#'basic.consume_ok'{consumer_tag = CTag}, State) ->
    lager:info("Consumer Tag: ~p", [CTag]),
    {noreply, State};
handle_info({#'basic.deliver'{consumer_tag = _CTag,
    delivery_tag = _DeliveryTag,
    exchange = _Exch,
    routing_key = _RK},
    #amqp_msg{payload = Data} = _Content} = AmqpPackage,
    #state{amqp_package_recv_count = Recv,
        channel = Channel,
        queue_info = QueueInfo,
        consumer_check_interval = ConsumerCheckInterval,
        consumer_msg_rate = MsgRate,
        is_unsubscribe = IsUnsubscribe,
        stat_module = StatModule,
        receiver_module = ReceiverModule} = State) ->

    Pid = whereis(ReceiverModule),
    State3 = case Pid of
                 undefined ->   %% waiting for ReceiverModule come up, should use timer:sleep()?
                     lager:warning("No Pid for ~p", [ReceiverModule]),
                     self() ! AmqpPackage,
                     State;
                 _ ->
                     {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
                     ConsumeMsgLen = ConsumerCheckInterval * MsgRate,
                     lager:debug("QueueLen ~p, config ~p", [Len, ConsumeMsgLen]),
                     State2 = case {Len > ConsumeMsgLen, IsUnsubscribe} of
                                  {true, false} ->
                                      lager:critical("msgq length ~p > config leng ~p", [Len, ConsumeMsgLen]),
                                      unsubscribe_incomming_queues(Channel, QueueInfo),
                                      lager:warning("Pause Consumer"),
                                      State#state{is_unsubscribe = true};
                                  _ ->
                                      State
                              end,
                     gen_server:cast(ReceiverModule, {package_from_mq, Data}),
                     case StatModule of
                         undefined ->
                             ignore;
                         _ ->
                             StatModule:notify({mqtt_mq_recv, {inc, 1}})  %% current use folsom
                     end,
                     State2#state{amqp_package_recv_count = Recv + 1}
             end,
    {noreply,State3};

handle_info({timeout, _Ref, check_consumer}, #state{channel = Channel,
    consumer_check_interval = ConsumerCheckInterval,
    consumer_msg_rate = Rate,
    consumer_last_stat = ConsumerLastStat,
    consumer_stat = ConsumerStat,
    is_unsubscribe = IsUnsubscribe,
    queue_info = QueueInfo,
    receiver_module = ReceiverModule} = State) ->
    erlang:cancel_timer(State#state.tref),
    Pid = whereis(ReceiverModule),
    {NewRate, CurrentStat} = case {ConsumerStat, ConsumerLastStat} of
                                 {{undefined, _}, _} ->
                                     {Rate, undefined};
                                 {{Mod, Fn}, _} ->
                                     ConsumerCurrentStat = Mod:Fn(),
                                     CurrentRate = case ConsumerCurrentStat > ConsumerLastStat of
                                                       true ->
                                                           ConsumerRate = (ConsumerCurrentStat - ConsumerLastStat) / ConsumerCheckInterval,
                                                           round(ConsumerRate + 0.5);
                                                       _ -> %% consumer stat be resetted
                                                           Rate
                                                   end,
                                     {CurrentRate, ConsumerCurrentStat}
    end,
    {message_queue_len, Len} = case Pid of
                                   undefined ->
                                       {message_queue_len, 0};
                                   _ ->
                                       erlang:process_info(Pid, message_queue_len)
                               end,
    ConsumeMsgLen = ConsumerCheckInterval * NewRate,

    State2 = case {Len > ConsumeMsgLen, IsUnsubscribe} of
                 {_, disable} ->  %% won't subscribe queue
                     State;
                 {true, true} ->
                     State;
                 {true, false} ->
                     unsubscribe_incomming_queues(Channel, QueueInfo),
                     State#state{is_unsubscribe = true};
                 {false, false} ->
                     State;
                 {false, true} ->
                     NewQueueInfo = [
                         {
                             ConsumeQueue,
                             amqp_channel:subscribe(Channel,
                                 #'basic.consume'{queue = ConsumeQueue,
                                     consumer_tag = ConsumerTag,
                                     no_ack = true},
                                 self())
                         }  || {ConsumeQueue, {_, ConsumerTag}} <- QueueInfo],
                     lager:warning("Resume Consumer \n old info ~p \n new info ~p", [QueueInfo, NewQueueInfo]),
                     State#state{is_unsubscribe = false, queue_info=NewQueueInfo}
             end,
    TRef = erlang:start_timer(ConsumerCheckInterval * 1000, self(), check_consumer),
    {noreply, State2#state{tref = TRef, consumer_msg_rate = NewRate, consumer_last_stat = CurrentStat}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State=#state{channel=Channel,
    is_unsubscribe = IsUnsubscribe,
    tref = TimeRef,
    queue_info = QueueInfo}) ->
    case IsUnsubscribe of
        false ->
            unsubscribe_incomming_queues(Channel, QueueInfo);
        _ ->
            ignore
    end,
    erlang:cancel_timer(TimeRef),
    lager:debug("proxy client terminated ~p", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

test() ->
    gen_server:call(self(),
        {forward_to_amqp, <<"amqp_proxy_client">>, <<"route">>, <<"message">>},
        infinity).
%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

amqp_publish(Exchange, RoutingKey, Message, Channel, State) ->
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Props = #'P_basic'{content_type = <<"application/octet-stream">>, expiration = <<"8000">>},
    Msg = #amqp_msg{payload = Message, props = Props},
    amqp_channel:cast(Channel, Publish, Msg),

    State.

config_val(C, Params, Default) ->
    case lists:keyfind(C, 1, Params) of
        {C, V} -> V;
        _ ->
            lager:info("Default: ~p", [Default]),
            Default
    end.

amqp_channel(AmqpParams) ->
    case maybe_new_pid({node(), AmqpParams, connection},
        fun() -> amqp_connection:start(AmqpParams) end) of
        {ok, Client} ->
            case maybe_new_pid({node(), AmqpParams, channel},
                fun() -> amqp_connection:open_channel(Client) end) of
                {ok, Channel} ->
                    {ok, Client, Channel};
                Error2 ->
                    Error2
            end;
        Error ->
            Error
    end.

maybe_new_pid(Group, StartFun) ->
    case pg2:get_closest_pid(Group) of
        {error, {no_such_group, _}} ->
            pg2:create(Group),
            maybe_new_pid(Group, StartFun);
        {error, {no_process, _}} ->
            case StartFun() of
                {ok, Pid} ->
                    pg2:join(Group, Pid),
                    erlang:monitor(process, Pid),
                    {ok, Pid};
                Error ->
                    Error
            end;
        Pid ->
            {ok, Pid}
    end.

unsubscribe_incomming_queues(Channel, QueueInfo) ->
    lists:map(fun(Info) ->
        case Info of
            {Queue, {'basic.consume_ok', ConsumerTag}} ->
                Method = #'basic.cancel'{consumer_tag = ConsumerTag},
                Result = amqp_channel:call(Channel, Method),
                lager:debug("Unsubscribe queue succee ~p Result ~p", [Queue, Result]);
            {Queue, {Other, _ComsumerTag}} ->
                lager:debug("Queue does not consumer ~p, ~p", [Queue, Other])
        end
    end, QueueInfo).

subscribe_incoming_queues(Key, Queue, Exchange, Channel, NodeTag) ->
    ConsumeQueue =
        case binary:last(Queue) of
            $_ ->
                <<Queue/binary, NodeTag/binary>>;
            _ ->
                Queue
        end,

    ConsumerTag = case amqp_channel:call(Channel, #'queue.declare'{queue = ConsumeQueue}) of
        #'queue.declare_ok'{} ->
            Tag = amqp_channel:subscribe(Channel,
                #'basic.consume'{queue = ConsumeQueue,
                    no_ack = true},
                self()),
            lager:debug("Tag: ~p", [Tag]),
            Tag;
        Return2 ->
            lager:error("declare queue failed: ~p", [Return2]),
            <<"tag-error">>
    end,

    KeyEnd = binary:last(Key),
    RoutingKey =
        case KeyEnd of
            $_ ->
                <<Key/binary, NodeTag/binary>>;
            _ ->
                Key
        end,

    Binding = #'queue.bind'{queue = ConsumeQueue,
        exchange = Exchange,
        routing_key = RoutingKey},
    case amqp_channel:call(Channel, Binding) of
        #'queue.bind_ok'{} ->
            lager:info("Bind succeeded: ~p",
                [{ConsumeQueue, Exchange, RoutingKey}]);
        Return3 ->
            lager:error("Bind failed: ~p",
                [{ConsumeQueue, Exchange, RoutingKey, Return3}])
    end,
    {ConsumeQueue, ConsumerTag}.

declare_and_bind_outgoing_queues(Channel, Exchange, OutgoingQueues) ->
    [
        {#'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
            #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{queue = Queue,
                exchange = Exchange,
                routing_key = Key})}
        || {Key, Queue} <- OutgoingQueues].
