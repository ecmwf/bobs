## Benchmark Results
Syntax:
```
python -m pestcli.benchmark <number of datasets> <mb per dataset> <parallel writers> <parallel readers>
```

### Unit test benchmark
```
python -m pestcli.benchmark 4 2 1 1
# python-server v0.1, within-host, no mem/disk restriction, 4kb page, 1k/2k page eviction
whole thing elapsed 0.349 sec
```

```
python -m pestcli.benchmark 1 128 1 1
# python-server v0.1, within-host, no mem/disk restriction, 4kb page, 1k/2k page eviction
whole thing elapsed 5.387 sec
```

```
python -m pestcli.benchmark 32 4 8 8
# python-server v0.1, within-host, no mem/disk restriction, 4kb page, 1k/2k page eviction
whole thing elapsed 4.252 sec
```

### One large file read & write, no eviction
```
python -m pestcli.benchmark 1 2048 1 1
# TODO
```

### Many medium files read & write, no eviction
```
python -m pestcli.benchmark 32 128 8 8
# TODO
```

### Multiple large files read & write, eviction
```
python -m pestcli.benchmark 8 512 2 2
# resource constraints: TODO
# TODO
```
