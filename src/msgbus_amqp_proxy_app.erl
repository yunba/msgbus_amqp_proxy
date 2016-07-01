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

-module(msgbus_amqp_proxy_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-compile({parse_transform, lager_transform}).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    ets:new(msgbus_amqp_clients_priority_table, [public, named_table, ordered_set]),
    {ok, Rabbitmqs} = application:get_env(rabbitmqs),
    {ok, OutgoingQueues} = application:get_env(outgoing_queues),
    {ok, IncomingQueues} = application:get_env(incoming_queues),
    {ok, NodeTag0} = application:get_env(node_tag),
    NodeTag = generate_broker_score(NodeTag0),
    msgbus_amqp_proxy_sup:start_link({Rabbitmqs, OutgoingQueues, IncomingQueues, NodeTag}).

stop(_State) ->
  ets:delete(msgbus_amqp_clients_priority_table),
  ok.

generate_broker_score(NodeTag) ->
    try
        {ok, BrokerScoreRegulation} = application:get_env(emqtt, broker_score_regulation),
        {host_provider_code, PredefinedHostProviderCode} = lists:keyfind(host_provider_code, 1, BrokerScoreRegulation),
        {zone_code, PredefinedZoneCode} = lists:keyfind(zone_code, 1, BrokerScoreRegulation),
        [Part1, _, Part2] = string:tokens(NodeTag, "-"),
        {HostProvider, Zone} = lists:split(1, Part1),
        {HostProvider, HostProviderCode} = lists:keyfind(HostProvider, 1, PredefinedHostProviderCode),
        {Zone, ZoneCode} = lists:keyfind(Zone, 1, PredefinedZoneCode),

        {Customer, FrontSeq} = case string:to_integer(lists:reverse(Part2)) of
                                   {S1, []} ->
                                       {"comm", list_to_integer(lists:reverse(erlang:integer_to_list(S1)))};
                                   {S2, Customer1} ->
                                       {lists:sublist(lists:reverse(Customer1), 4), list_to_integer(lists:reverse(integer_to_list(S2)))};
                                   _Else ->
                                       lager:debug("illegal node tag ~p~n", NodeTag)
                               end,

        CustomerBin = lists:foldl(fun(_X, Bin) -> <<16#00, Bin/binary>> end, list_to_binary(Customer), lists:seq(1, 4 - size(list_to_binary(Customer)))),
        FrontSeq1 = lists:foldl(fun(_X, Bin) -> <<16#00, Bin/binary>> end, <<FrontSeq>>, lists:seq(1, 2 - size(<<FrontSeq>>))),
        Reserved = <<16#00>>,

        lager:debug("HostProviderCode = ~p, ZoneCode = ~p, Customer = ~p, FrontSeq = ~p~n", [HostProviderCode, ZoneCode, CustomerBin, FrontSeq1]),

        BrokerScore = <<HostProviderCode, ZoneCode, CustomerBin/binary, FrontSeq1/binary, Reserved/binary>>,

        lager:debug("BrokerScore = ~p~n", [BrokerScore]),

        BrokerScore
    catch
        _Type:Error ->
            lager:critical("runtime error:~p, generate broker score for ~p failed !", [Error, NodeTag]),
            undefined
    end.