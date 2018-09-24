-module(s3am).

-compile({parse_transform, category}).
-include_lib("datum/include/datum.hrl").

-export([
   get/1,
   fetch/1,
   put/2,
   objects/1
]).


%%
%% 
-spec get(uri:uri()) -> datum:stream().

get({uri, s3, _} = Uri) ->
   [either ||
      Bucket =< s3_bucket(Uri),
      Object =< s3_object(Uri),
      erlcloud_aws:auto_config(),
      cats:unit( erlcloud_s3:make_get_url(3600, Bucket, Object, _) ),
      knet:connect(_, #{active => 1024}),
      stream(_)
   ];

get(Uri)
 when is_list(Uri) orelse is_binary(Uri) ->
   s3am:get(uri:new(Uri)).

stream(Sock) ->
   stream(Sock, infinity).

stream(Sock, Timeout) ->
   case knet:recv(Sock, Timeout) of
      {ioctl, _, _} ->
         stream(Sock, Timeout);

      {_, Sock, passive} ->
         knet:ioctl(Sock, {active, 1024}),
         stream(Sock, Timeout);

      {_, Sock, eof} ->
         knet:close(Sock),
         stream:new();

      {_, Sock, {error, _} = Error} ->
         knet:close(Sock),
         stream:new(Error);

      {_, _, Pckt} ->
         stream:new(Pckt, fun() -> stream(Sock, Timeout) end)
   end.

%%
%%
-spec fetch(uri:uri()) -> datum:stream().

fetch(Uri) ->
   case s3am:get(Uri) of
      #stream{head = {200, _, _}} = Stream ->
         {ok, stream:tail(Stream)};
      #stream{head = {404, _, _}} = Stream ->
         _Reason = scalar:s( stream:list( stream:tail(Stream) ) ),
         {error, not_found};
      #stream{head = {Code, _, _}} = Stream ->
         Reason = scalar:s( stream:list( stream:tail(Stream) ) ),
         {error, {Code, Reason}}
   end.

%%
%%
-spec put(uri:uri(), datum:stream()) -> datum:either().

put({uri, s3, _} = Uri, Stream) ->
   [either ||
      Bucket =< s3_bucket(Uri),
      Object =< s3_object(Uri),
      Config <- erlcloud_aws:auto_config(),
      erlcloud_s3:start_multipart(Bucket, Object, [], [], Config),
      Upload =< lens:get(lens:pair(uploadId), _),
      Chunks =< stream:list( upload(Bucket, Object, Upload, Stream, Config) ),
      erlcloud_s3:complete_multipart(Bucket, Object, Upload, Chunks, [], Config)
   ];

put(Uri, Stream)
 when is_list(Uri) orelse is_binary(Uri) ->
   s3am:put(uri:new(Uri), Stream).


upload(Bucket, Object, Upload, Stream, Config) ->
   stream:map(
      fun([Id, Chunk]) ->
         {ok, ETag} = erlcloud_s3:upload_part(Bucket, Object, Upload, Id, Chunk, [], Config),
         {Id, lens:get(lens:pair(etag), ETag)}         
      end,
      stream:zip(stream:build(1), chunks(Stream))
   ).

chunks(Stream) ->
   stream:unfold(fun chunk/1, Stream).

chunk(Stream) ->
   chunk(0, [], Stream).

chunk(_, [], ?stream()) ->
   stream:new();

chunk(_, Chunk, ?stream()) ->
   {lists:reverse(Chunk), stream:new()};

chunk(N, Chunk, Stream) 
 when N >= 5242880 ->
   {lists:reverse(Chunk), Stream};

chunk(N, Chunk, Stream) ->
   Head = stream:head(Stream),
   chunk(N + size(Head), [Head|Chunk], stream:tail(Stream)).

%%
%%
-spec objects(uri:uri()) -> datum:stream().

objects({uri, s3, _} = Uri) ->
   stream:unfold(fun s3_objects/1, {s3_bucket(Uri), s3_object(Uri), undefined});

objects(Uri)
 when is_list(Uri) orelse is_binary(Uri) ->
   s3am:objects(uri:new(Uri)).


s3_objects({Bucket, Prefix, undefined}) ->
   s3_objects({Bucket, Prefix, s3_lookup(Bucket, Prefix, undefined)});

s3_objects({_, _, []}) ->
   undefined;

s3_objects({Bucket, Prefix, [H]}) ->
   {uri:path([$/|H], uri:host(Bucket, uri:new(s3))), 
      {Bucket, Prefix, s3_lookup(Bucket, Prefix, H)}};

s3_objects({Bucket, Prefix, [H|T]}) ->
   {uri:path([$/|H], uri:host(Bucket, uri:new(s3))),
      {Bucket, Prefix, T}}.

s3_lookup(Bucket, Prefix, Seq) ->
   {ok, Config} = erlcloud_aws:auto_config(),
   Json = erlcloud_s3:list_objects(Bucket, s3_lookup_query(Prefix, Seq), Config),

   lens:get(
      lens:c(
         lens:pair(contents),
         lens:traverse(),
         lens:pair(key)
      ),
      Json
   ).

s3_lookup_query(Prefix, undefined) ->
   [{prefix, Prefix}];
s3_lookup_query(Prefix, Seq) ->
   [{prefix, Prefix}, {marker, Seq}].




%%
%%
s3_bucket(Uri) ->
   scalar:c(uri:host(Uri)).

s3_object(Uri) ->
   [$/|Object] = scalar:c(uri:path(Uri)),
   Object.
