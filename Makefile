all: deps compile

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

generate:
	./rebar generate -f

relclean:
	rm -rf rel/msgbus_amqp_proxy

run: compile
	erl -pa ebin/ -pa deps/*/ebin/ -s msgbus_amqp_proxy
