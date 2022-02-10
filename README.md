# redis-sampler

This project is a tool for analyzing Redis data that also capable for deleting keys based on given patterns. 

## Usage

```sh
node sampler.js "<Key Pattern>" --host=<Redis IP> --port=<Redis Port> --display-max-levels=<Number of key grouping levels to display>
```

Example:

```sh
node sampler.js "es_stats:*:2017_*" --host=52.208.47.17 --port=6655 --display-max-levels=1
```

The example command above will display all keys that match the pattern `es_stats:*:2017_*` and will group them by the first level of the key like this : 

```
{SIZE: 688.12MB, COUNT: 4363188, AVG: 0.16KB
  es_stats: {SIZE: 688.12MB, COUNT: 4363188, AVG: 0.16KB
    activeHouseholds: {SIZE: 23.72MB, COUNT: 248353, AVG: 0.10KB}
    appAmountOpened: {SIZE: 20.77MB, COUNT: 145203, AVG: 0.14KB}
    appLaunchesPerCustomer: {SIZE: 12.39MB, COUNT: 119853, AVG: 0.10KB}
    ...
  }
}
```

## Deleting keys

In order to delete keys based on a pattern, `--delete` flag must be provided in addition to the other parameters. After providing `--delete` flag, an output like below will be displayed :

```
Deleting is dangerous. Please check all arguments and specify the following delete security hash: f740f719a932fc5e8776aa9e743bb8b8
```

Because deleting keys is a dangerous operation, the security has generated by the above command must be used for the actual delete operation like this :

```
node sampler.js "es_stats:*:2017_*" --host=52.208.47.17 --port=6655 --display-max-levels=1 --delete=f740f719a932fc5e8776aa9e743bb8b8
```

> While deleting keys, the master/write endpoint of the Redis server must be provided for the host parameter.
