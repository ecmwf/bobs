#!/bin/bash
# minimalistic curl-based client for save/read

URL_ROOT="http://localhost:8010/"

case "$1" in
    create)
        COMMAND="create" ; shift ;;
    write)
        COMMAND="write" ; shift ;;
    close)
        COMMAND="close" ; shift ;;
    read)
        COMMAND="read" ; shift ;;
    *)
        echo "unknown command $1"
        exit 1 ;;
esac
    
SUFFIX=""
PAYLOAD=""
OFFSET=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--data)
            if [[ $COMMAND != "write" ]] ; then echo "-d|--data supported only for 'write' command" ; exit 1 ; fi
            if [[ -n "$PAYLOAD" ]]  ; then echo "--, -d|--, and -f|--file mutually exclusive" ; exit 1 ; fi
            PAYLOAD="--data $2"
            shift
            shift
            ;;
        -f|--file)
            if [[ $COMMAND != "write" ]] ; then echo "-f|--file supported only for 'write' command" ; exit 1 ; fi
            if [[ -n "$PAYLOAD" ]] ; then echo "--, -d|--, and -f|--file mutually exclusive" ; exit 1 ; fi
            PAYLOAD="--data-binary @$2"
            shift
            shift
            ;;
        -o|--offset)
            if [[ $COMMAND != "write" ]] ; then echo "-o|--offset supported only for 'write' command" ; exit 1 ; fi
            OFFSET="/$2"
            shift
            shift
            ;;
        -k|--key)
            if [[ $COMMAND = "create" ]] ; then echo "-k|--key unsupported for 'create' command" ; exit 1 ; fi
            SUFFIX="/$2"
            shift
            shift
            ;;
        --)
            if [[ $COMMAND != "write" ]] ; then echo "-- supported only for 'write' command" ; exit 1 ; fi
            if [[ -n "$PAYLOAD" ]] ; then echo "--, -d|--, and -f|--file mutually exclusive" ; exit 1 ; fi
            PAYLOAD="--data-binary @-"
            shift
            shift
            ;;
        -u|--url)
            URL_ROOT="$2"
            shift
            shift
            ;;
        -h|--help)
            echo "read the code"
            exit 0
            ;;
        *)
            echo "unknown parameter $1"
            exit 1 ;;
    esac
done

if [[ "$COMMAND" != "create" ]]  && [[ -z "$SUFFIX" ]] ; then echo "for '$COMMAND', -k|--key must be used" ; exit 1 ; fi
if [[ "$COMMAND" = "write" ]]  && [[ -z "$PAYLOAD" ]] ; then echo "for 'write', -d|--data or -f|--file or -- must be used" ; exit 1 ; fi
if [[ "$COMMAND" = "write" ]]  && [[ -z "$OFFSET" ]] ; then OFFSET="/0" ; fi

call_write () {
    # $1 url $2 payload
    curl $2 -XPOST "$1"
}

call_read () {
    # $1 url
    curl -XGET "$1/0/0"
}

call_create() {
    # $1 url
    curl -XPUT "$1"
}

call_close() {
    # $1 url
    curl -XPOST "$1"
}

"call_$COMMAND" "$URL_ROOT$COMMAND$SUFFIX$OFFSET" "$PAYLOAD"
