compile method:
```
gcc -std=c99 -g -O0 -fPIC -shared udf_percent.c -o libudf_percent.so
```

create udf for TDengine:
```
create aggregate function udf_percent as '/etc/taos/udf/libudf_percent.so' outputtype DOUBLE bufsize 8;
```
