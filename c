#!/bin/sh
set -e
envfn=$(dirname $0)/../goracle/env
if [ -e "$envfn" ]; then
    . "$envfn"
fi
dsn=${DSN}
if [ -z "$dsn" ]; then
    dsn=$(cat ../goracle/.dsn)
fi

go test -connect=${dsn} ./...
go build

{
if echo "$dsn" | grep -q '@XE'; then
    ./oracall -F <${1:-one.csv}
else
    ./oracall -F -connect="$dsn" ${2:-DB_WEB.SENDPREOFFER_31101}
fi
} >examples/minimal/generated_functions.go
go build ./examples/minimal
echo
echo '-----------------------------------------------'
CMD='./minimal -connect='${dsn}" ${2:-DB_web.sendpreoffer_31101}"
echo "$CMD"
#$CMD '{"p_lang":"hu", "p_sessionid": "123", "p_kotveny_vagyon":{"teaor": "1233", "forgalom": 0}, "p_telep":[{"telep_azon":"A", "telep_kod":"C"},{"telep_azon":"z", "telep_kod":"x"}]}'
time $CMD '{"p_lang":"hu", "p_sessionid": "123", "p_kotveny_vagyon":{"teaor": "1233", "forgalom": 0}, "p_telep":[{"telep_azon":1, "telep_kod":0}], "p_kotveny": {"dijfizgyak":"N"}, "p_kedvezmenyek": ["KEDV01"]}'
